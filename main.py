import boto3
import csv
import logging
# Function 01: data handling
from botocore.exceptions import ClientError


def dataHandling(Line):
    # Algorithm steps:
    # 1- check the begining Pxx ==> 04 cases:
    # 2- Apply special corrections for specific columns:
    #   - Remove leading zeros from user_type_id: 003 ==> 3
    #   - Replace P05 & P06 by: Montant_ht et TVA respectivelly
    #   - Suppose P07 = P06 & check with source later
    #   - Transpose 02 lines (in ventes transactions) to 01 line
    # 3- Save each data line into corresponding table using table index
    # 4- Check that all id products in ventes are existing in products list, otherwise add them without labels, and check with source later

    global lineVente
    global idProdVentes
    global idProdList
    # Extract dataSetID:
    dataSetID = Line[0:3]
    # Convert Line from obj to string
    Line = str(Line)
    # Split Line string to get list of strings using delimeter |
    Line = Line.split('|')
    if dataSetID == "P01":
        # Special handling for user type: remove leading zeros
        Line[4] = Line[4].lstrip("0")
        # Delete the 1st element in the list (Pxx)
        del Line[0]
        saveDataToCSV(Line,1)
        print("linge users ajoutée")
    elif dataSetID == "P02":
        # Special handling for user type: remove leading zeros
        Line[1] = Line[1].lstrip("0")
        # Delete the 1st element in the list (Pxx)
        del Line[0]
        saveDataToCSV(Line, 2)
        print("ligne users types ajoutée")

    elif dataSetID == "P03":
        # if line contain P05 save it
        # if line contain P06 ou P07 replace P05 by tva number
        if "P05" in Line:
            lineVente = []
            lineVente = Line[:]
        elif "P06" in Line or "P07" in Line:
            lineVente[5] = Line[4]
            del lineVente[0]
            saveDataToCSV(lineVente, 3)
            # Collect id produit in ventes:
            idProdVentes.append(lineVente[1])
            print("ligne de ventes ajoutée")
        else:
            print("Pas de code d'attribut: ligne de donnée ignorée")

    elif dataSetID == "P04":
        # Delete the 1st element in the list (Pxx)
        del Line[0]
        saveDataToCSV(Line, 4)
        # Collect id produit in products list:
        idProdList.append(Line[0])
        print("ligne de produits ajoutée")

    elif dataSetID == "P05":
        print("L'attribut Montant_ht")
    elif dataSetID == "P06":
        print("L'attribut TVA ")
    else:
        print("dataSetId inconnue")




# Function 02: Create Header of csv files:
def CreateHeadersOfCSV():
    # tbl_users
    header_users = ["user_id", "user_nom","user_prenom","user_type_id"]
    with open("tbl_users.csv", 'w', newline='', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header_users)
        f.close()
    # tbl_users_type
    header_users_type = ["user_type_id", "user_type"]
    with open("tbl_users_type.csv", 'w', newline='', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header_users_type)
        f.close()
    # tbl_ventes
    header_ventes = ["id_user","id_produit","qte","montant_ht","tva","Date"]
    with open("tbl_ventes.csv", 'w', newline='', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header_ventes)
        f.close()
    # tbl_produits:
    header_produits = ["id_produit","nom_produit"]
    with open("tbl_produits.csv", 'w', newline='', encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header_produits)
        f.close()




# Function 03: Saving datasets in csv files:
def saveDataToCSV(row,table_index):
    if table_index == 1:
        with open("tbl_users.csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # write a row to the csv file
            writer.writerow(row)
            # close the file
            f.close()
    elif  table_index == 2:
        with open("tbl_users_type.csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # write a row to the csv file
            writer.writerow(row)
            # close the file
            f.close()
    elif table_index == 3:
        with open("tbl_ventes.csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # write a row to the csv file
            writer.writerow(row)
            # close the file
            f.close()
    elif table_index == 4:
        with open("tbl_produits.csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # write a row to the csv file
            writer.writerow(row)
            # close the file
            f.close()
    else:
        print("Incorrect table index !")

# Function 4: add missed product's id:
def addMissingProducts():
    global idProdList
    global idProdVentes
    for pv in set(idProdVentes): # set is used to get uniques IDs products
        if pv not in idProdList:
            print("Produit {} vendu et non trouvé dans la liste des produits".format(pv))
            # add this product into products list:
            newProd = [pv, ""]
            with open("tbl_produits.csv", 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(newProd)
                f.close()
            print("Produit {} ajouté dans la liste des produits.".format(pv))

# Début du programme principale

# 1- Declare Global Variable:
lineVente = [] # empty list
idProdVentes = [] # empty list for id products found in ventes transactions
idProdList = []   # empty list for id products found in initial data set of products
# 2- AWS S3 connection intialization:
s3 = boto3.resource(
service_name = 's3',
region_name = 'eu-west-3',
aws_access_key_id = 'AKIA4LU6T2W46CJD3FML',
aws_secret_access_key = '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')

# 3- Check list buckets
for bucket in s3.buckets.all():
    print(bucket.name)

# 4- Create Headers of CSV files before handling and saving them:
CreateHeadersOfCSV()

# 5- List all objects (our source text files) in all buckets and start handling data:
for obj in bucket.objects.all():
    key = obj.key
    print("key = " + key)
    # Search for our source file: ventes.txt
    if obj.key == "ventes.txt":
        allText = obj.get()['Body'].readlines()
        #print(body)
        for line in allText:
            # Line is a byte class, should be convert to normal string
            line = line.decode("utf-8")
            # Call the main function in order to handle data (split, modeling, prep, save into csv subsets, ...etc)
            dataHandling(line.strip())

# 6- Add missing products:
addMissingProducts()

# 7- Upload csv files to the our S3 Bucket:
s3.meta.client.upload_file("tbl_users.csv","case1files","tbl_users.csv")
print("tbl_users.csv ==> uploaded ")
s3.meta.client.upload_file("tbl_users_type.csv","case1files","tbl_users_type.csv")
print("tbl_users_type.csv ==> uploaded ")
s3.meta.client.upload_file("tbl_ventes.csv","case1files","tbl_ventes.csv")
print("tbl_ventes.csv ==> uploaded ")
s3.meta.client.upload_file("tbl_produits.csv","case1files","tbl_produits.csv")
print("tbl_produits.csv ==> uploaded ")







