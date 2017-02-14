__author__ = 'tanuj'
'''
#running the script:  sf.py <parameter( pass upload or download)>
#script need two files: sf_login.json, sf_metadata.json
#sf_login.json looks like
{"url":"https://avvo--devprotest.cs42.my.salesforce.com/services/oauth2/token","grant_type":"password","client_id":"<check in SF>"","client_secret":"<check in SF>","username":"<sf user>","password":"<sf pw>"}
client_id etc. you can check in sf (Apps Setup ->create->App)
#sf_metadata.json looks like
{"sf_object":"Account","file_path":"../sf_download.csv","query":"select firstname,lastname,Professional_ID__c from Account"}
'''

try:
    import os
    import sys
    import json
    from salesforce_bulk import SalesforceBulk
    from urlparse import urlparse
    from salesforce_bulk import CsvDictsAdapter
    import csv
    import time

except ImportError as e:
    print ("Error importing Modules", e)
    sys.exit(1)

def gen_chunks(reader, chunksize=10000):
    """
    Chunk generator. Take a CSV `reader` and yield
    `chunksize` sized slices.
    """
    chunk = []
    for i, line in enumerate(reader):
        if (i % chunksize == 0 and i > 0):
            yield chunk
            del chunk[:]
        chunk.append(line)
    yield chunk

def upload(sf_object,file_path):
    #Extracting SF data from csv file
    # open the file
    with open(file_path, 'r') as file_handler:
        csv_reader = csv.DictReader(file_handler, delimiter='|')
        for chunk in gen_chunks(csv_reader): # loading 10k chunk, needed due to SF limit
            job = bulk.create_upsert_job(sf_object, 'Professional_ID__c',contentType='CSV')
            csv_iter = CsvDictsAdapter(iter(chunk))
            batch = bulk.post_bulk_batch(job, csv_iter)
            bulk.wait_for_batch(job, batch)
            bulk.close_job(job)
            print (sf_object+" upload completed")

def download(sf_object,file_path,query):
    #Downloading SF data
    job = bulk.create_query_job(sf_object, contentType='CSV')
    batch = bulk.query(job,query)
    while not bulk.is_batch_done(job, batch):
        time.sleep(10)
        bulk.close_job(job)

    with open(file_path,'wb') as f:
       for row in bulk.get_batch_result_iter(job, batch, parse_csv=False):
           f.write(str(row).replace('"',""))
           f.write("\n")
    bulk.close_job(job)
    print (sf_object+" download completed")

def metadata_error(error_reason):
    if (error_reason=="missing metadata"):
        print("variables in metadata file were missing or typed wrongly")
    else:
        print ("missed passing script parameter (upload or download)")
    sys.exit(1)

def error_handler(type,value,obj,step_message):
    sys.stdout.write("failure step: %s\n" %step_message  )
    sys.stdout.write("error type: %s\n" % type )
    sys.stdout.write("error message: %s\n" % value )
    sys.stdout.write("traceback: %s\n" % obj )
    sys.exit(1)


try:
 if __name__ =='__main__':
    LOAD_TYPE=sys.argv[1]
    SF_LOGIN_FILE='sf_login.json'
    SF_METADATA_FILE='sf_metadata.json'

    step_message="loading sf login file"
    #salesforce login credentials
    with open(SF_LOGIN_FILE) as login:
        login_record = json.load(login)
    #Constants
    URL=login_record['url']
    print URL
    GRANT_TYPE=login_record['grant_type']
    CLIENT_ID=login_record['client_id']
    CLIENT_SECRET=login_record['client_secret']
    USERNAME=login_record['username']
    PASSWORD=login_record['password']

    step_message="running sf authentication"
    #Salesforce authentication
    oauth=('curl  -v '+URL+' -d '+' "grant_type= '+GRANT_TYPE+'" -d '+' "client_id= '+CLIENT_ID+'" -d '+' "client_secret= '+CLIENT_SECRET+'" -d '+' "username= '+USERNAME+'" -d '+' "password= '+PASSWORD+'"')
    sf_oauth = os.popen(oauth).read()
    token_dict=json.loads(sf_oauth)
    session_id=token_dict[u'access_token']
    instance_url=token_dict[u'instance_url']
    bulk = SalesforceBulk(sessionId=session_id, host=urlparse(instance_url).hostname)

    #opening SF metadata file
    step_message="opening metadata file"
    with open (SF_METADATA_FILE) as meta:
        meta_record=json.load(meta)

    SF_OBJECT=meta_record.get('sf_object',None)
    FILE_PATH=meta_record.get('file_path',None)
    QUERY=meta_record.get('query',None) # applicable only for downloading files

    #calling function

    if LOAD_TYPE == 'upload':
        if None in (SF_OBJECT,FILE_PATH):
            metadata_error("missing metadata")
        else:
            print("sf uploading started")
            upload(SF_OBJECT,FILE_PATH)
    elif LOAD_TYPE == 'download':
        if None in (SF_OBJECT,FILE_PATH,QUERY):
            metadata_error("missing metadata")
        else:
            print("sf downloading started")
            download(SF_OBJECT,FILE_PATH,QUERY)
    else:
        metadata_error("missing script parameter")

except:
    error_message = sys.exc_info()
    error_handler(error_message[0], error_message[1], error_message[2], step_message)
