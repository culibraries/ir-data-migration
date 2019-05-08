import pandas,json,sys
from time import sleep
import requests,os
import boto3,botocore
import re

catalog_url="https://libapps.colorado.edu/api/catalog/data/catalog/cuscholar"
api_token=os.getenv('API_TOKEN')
headers={"Content-Type":"application/json","Authorization":"Token {0}".format(api_token)}
print (headers)
count=0
s3 = boto3.resource('s3')
s3_bucket='cubl-ir'

def checkSame(row1,row2):
    for key,value in row1.items():
        if not row1[key]==row2[key]:
            #print(row1[key],"different",row2[key],row1["front_end_url"])        
            return False
    return True

def postCatalogRecord(data):
    req=requests.post("{0}.json".format(catalog_url),data=json.dumps(data),headers=headers)
    if req.status_code > 400:
        raise Exception(req.text)
    #print (req.text)
    global count
    count+=1
    print(req.status_code," : ",count)

def getCatalogRecord(url):
    query={"filter":{"front_end_url":url}}
    req=requests.get("{0}.json?query={1}".format(catalog_url,json.dumps(query)),headers=headers)
    data=req.json()
    #print(data)
    if data['count']==0:
        return False
    return data['results'][0]
def s3_key_exists(bucket,key):
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    return True

def set_name_pdf(name):
    return name[0].replace('"','').replace("'",'')

def put_files_s3(data,bucket=s3_bucket):
    #s3 = boto3.resource('s3')
    req =requests.get(data['download_url'], allow_redirects=True)
    d = req.headers['content-disposition']
    fname = set_name_pdf(re.findall("filename=(.+)", d))
    key ="original/{0}/{1}".format(data['context_key'],fname)
    if not s3_key_exists(bucket,key):
        s3.Bucket(bucket).put_object(Key=key, Body=req.content)
        if 'data_files' in data:
            data['data_files']['s3']={"bucket":bucket,"key":key}
        else:
            data['data_files']={'s3':{"bucket":bucket,"key":key}}
    else:
        message="File already uploaded"
        if 'data_files' in data:
            data['data_files']['s3']={"bucket":bucket,"key":key,"message":message}
        else:
            data['data_files']={'s3':{"bucket":bucket,"key":key,"message":message}}
    if data['supplemental_filenames'].strip():
        afiles_list=data['supplemental_filesizes'].split(',')
        key_list=[]
        for idx, val in enumerate(afiles_list):
            req =requests.get("{0}&type=additional&filename={1}".format(data['download_url'],idx), allow_redirects=True)
            d = req.headers['content-disposition']
            fname = set_name_pdf(re.findall("filename=(.+)", d))
            key ="original/{0}/{1}/{2}".format(data['context_key'],'additional_files',fname)
            key_list.append(key)
            s3.Bucket(bucket).put_object(Key=key, Body=req.content)
        data['data_files']['s3']['additional_files']=key_list
    return data['data_files']

def check_advisors(str_list):
    return list(filter(None, str_list))

def runMetadataFile(df):
    pub=0
    dup_urls=[]
    rowold=None
    for i in df.index:
        row=df.loc[i].to_dict()
        record=getCatalogRecord(row["front_end_url"])
        if record:
            if original_data_load:
                id= record.pop('_id',None)
                if 'additional_records' in record:
                    break
                same=checkSame(record,row)
                if not same:
                    record['additional_records']=row
                    record['_id']=id
                    postCatalogRecord(record)
                    dup_urls.append(row["front_end_url"])
                    break
                else:
                    break
            else:
                row['_id']=record.pop('_id',None)
        row['keywords']=row['keywords'].split(',')
        row['keywords']=[x.strip() for x in row['keywords']]
        row['native_filesize']=int(row['native_filesize'])
        row['pdf_filesize']=int(row['pdf_filesize'])
        row['data_files']= put_files_s3(row)
        row['advisors']=check_advisors([row['advisor1'].strip(),row['advisor2'].strip(),row['advisor3'].strip(),row['advisor4'].strip(),row['advisor5'].strip()])
        pub+=1
        postCatalogRecord(row)
    print(pub)
    print("urls: ",len(dup_urls),dup_urls)

if __name__ == "__main__":
    original_data_load=False
    filename="20190208cuscholar_inventory.csv"
    if len(sys.argv) >1:
        filename=sys.argv[1]
    df=pandas.read_csv(filename,converters={i: str for i in range(0, 83)})
    df = df.drop_duplicates()
    runMetadataFile(df)