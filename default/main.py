from flask import Flask
import os
import sys
import glob
import string
import random
import redis
import logging
from gcloud import storage, pubsub
from google.cloud import logging

PROJECT_ID = 'transcode-159215'
TOPIC = 'projects/{}/topics/message'.format(PROJECT_ID)

logclient = logging.Client()
logger = logclient.logger( "ffmpeg-pool" )

app = Flask(__name__)
app.config[ "SECRET_KEY" ] = "test"
app.debug = True

class RedisQueue(object):
    
    def __init__(self, name, namespace='queue'):
       self.__db = redis.Redis(host="redis-11670.c10.us-east-1-4.ec2.cloud.redislabs.com", port=11670)
       self.key = '%s:%s' %(namespace, name)

    def qsize(self):
        return self.__db.llen(self.key)

    def empty(self):
        return self.qsize() == 0

    def put(self, item):
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        return self.get(False)


@app.route( "/readlog" )
def readLog():
    msg = ""

    try: 
	for entry in logger.list_entries():
            msg = msg + entry.payload + "<br>"
        logger.delete()
    except:
        msg = ""
    
    return msg


@app.route( "/cleantopic" )
def cleanTopics():
    client = pubsub.Client( PROJECT_ID )
    topic = client.topic( "ffmpeg-pool" )
    topic.delete()
    topic.create()

    return "Cleaned topic"


@app.route( "/transcode" )
def transcode():
    rqueue = RedisQueue('test')
    client = storage.Client( PROJECT_ID )
    bucket = client.bucket( PROJECT_ID + ".appspot.com" )
    blob = bucket.blob( "sample.mp4" )
    
    with open( "/tmp/sample2.mp4", 'w' ) as f:
        blob.download_to_file( f )
    
    os.system( "rm -f /tmp/chunk*" )
    ret = os.system( "ffmpeg -i /tmp/sample2.mp4 -map 0:a -map 0:v -codec copy -f segment -segment_time 10 -segment_format matroska -v error '/tmp/chunk-%03d.orig'")

    if ret:
        return "Failed"

    for rfile in glob.glob( "/tmp/chunk*" ):
        logger.log_text( "Server: Uploading file chunk: " + rfile )
        blob = bucket.blob( os.path.basename( rfile ) )
        blob.upload_from_file( open( rfile ) )
        rqueue.put( os.path.basename( rfile ) )
    
    pubsub_client = pubsub.Client( PROJECT_ID )
    topic = pubsub_client.topic( "ffmpeg-pool" )

    if not topic.exists():
        topic.create()

    topic.publish( "transcode" )

    return "Job queued for transcoding"


@app.route( "/" )
def home():
    return "<a href='/transcode'>/transcode</a> | <a href='/cleantopic'>/cleantopic</a> | <a href='/readlog'>/readlog</a>"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

