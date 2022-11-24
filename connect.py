import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
#import pyarrow.parquet as pq
import io

def initialize_storage_account(storage_account_name, storage_account_key):
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        print(e)
    return service_client

def get_directory_names():
    try:
        file_system_client = service_client.get_file_system_client(file_system="devfolder")
        paths = file_system_client.get_paths(path="assortment_internship_parquet/resources/")
        file_paths =list()
        for path in paths:
            if str(path).count("/")== 2:
                fName = str(path.name)
                file_paths.append(fName[40:])
    except Exception as e:
     print(e)
    return file_paths

def get_file_names(dir):
    try:
        file_system_client = service_client.get_file_system_client(file_system="devfolder")
        paths = file_system_client.get_paths(path="assortment_internship_parquet/resources/"+ dir)
        file_paths =list()
        for path in paths:
            fName = str(path.name)
            file_paths.append(fName)
    except Exception as e:
     print(e)
    return file_paths

def download_file_from_directory(file,dirName):
    try:
        print(file)
        parent_dir = "./in"
        path = os.path.join(parent_dir,dirName) 
        file_system_client = service_client.get_file_system_client(file_system="devfolder")
        directory_client = file_system_client.get_directory_client("assortment_internship/assortment_demo_files/"+dirName+"/")       
        local_file = open(path+file,'wb')
        file_client = directory_client.get_file_client(file)
        download = file_client.download_file()
        print(download)
        downloaded_bytes = download.readall()
        local_file.write(downloaded_bytes)
        local_file.close()
    except Exception as e:
     print(e)

adl=initialize_storage_account('dldevfolder', 'token')
dirNames= get_directory_names()
l=list()
for d in dirNames:
    li=get_file_names(d)
    l.append(li)

for f in l:
    for m in f:
        pos=m.rfind('/')
        fileName=m[pos+1:]
        index = l.index(f)
        download_file_from_directory(fileName,dirNames[index])


