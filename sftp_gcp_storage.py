import os
import base64
from base64 import decodebytes
import pysftp
from google.cloud import storage
import logging
import tempfile


storage_client = storage.Client()

def main(event, context=None):
    cnopts = pysftp.CnOpts()
    # cnopts.knownhosts = 'known_hosts' 
    cnopts.hostkeys = None
    pubsub_message = str(base64.b64decode(event['data']).decode('utf-8'))
    arr_pubsub_message=pubsub_message.split(":")
    filename=arr_pubsub_message[1]
    print(filename)
      
    try:
        s = pysftp.Connection('10.128.X.x', username='user', password='pass', cnopts=cnopts)
        remotepath1='userfala/files/'.format(filename)
        localpath1='/tmp/{}'.format(filename)
        logging.info(remotepath1)
        logging.info('get remote file')

        s.get(remotepath1,localpath1, preserve_mtime=True)
        s.close()
    except Exception as e:
        logging.info(e)
        print (str(e))

    #filename="fala-csv-output8947293584765891620.csv"
    gcs_bucket = storage_client.bucket('bucket_fala')
    blob = gcs_bucket.blob(filename)
    blob.upload_from_filename(localpath1)   
    logging.info("upload a bucket")
    

