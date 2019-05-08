import pandas,json,sys
import numpy as np
from time import sleep
import requests,os

catalog_url="https://libapps.colorado.edu/api/catalog/data/catalog/cuscholar/"
api_token=os.getenv('API_TOKEN')
headers={"Content-Type":"application/json","Authorization":"Token {0}".format(api_token)}

count=0

def checkSame(row1,row2):
    for key,value in row1.items():
        if not row1[key]==row2[key]:
            print(row1[key],"different",row2[key],row1["front_end_url"])        
            return False
    return True

def postCatalogRecord(data):
    req=requests.post(catalog_url,data=json.dumps(data),headers=headers)
    global count
    count+=1
    print(req.status_code," : ",count)

def getCatalogRecord(url):
    query={"filter":{"front_end_url":url}}
    req=requests.get("{0}.json?query={1}".format(catalog_url,json.dumps(query)),headers=headers)
    data=req.json()
    if data['count']==0:
        return False
    return data['results'][0]

def setStats(df,filetype):
    for i in df.index:
        row=df.loc[i].to_dict()
        record=getCatalogRecord(row["URL"].replace("http://","https://"))
        if record:
            if not 'stats' in record:
                record['stats']={}
            if filetype=="download":
                record['stats']['downloads']=row['Number of downloads']
                postCatalogRecord(record)
            elif filetype == "view":
                record['stats']['published']=row['First published']
                record['stats']['views']=row['Total']
                postCatalogRecord(record)

if __name__ == "__main__":
    #Title,URL,File type,Filename,Download link,Date first posted,Number of downloads
    filename="CU_Scholar_Fulltext.csv"
    filetype="download"
    if len(sys.argv) >2:
        filename=sys.argv[1]
        filetype= sys.argv[2]
    df=pandas.read_csv(filename,converters={i: str for i in range(0, 7)})
    df = df.drop_duplicates()
    df['URL'].replace('', np.nan, inplace=True)
    df.dropna(subset=['URL'], inplace=True)
    setStats(df,filetype=filetype)
