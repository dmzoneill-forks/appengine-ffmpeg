import os
from gcloud import storage, pubsub, logging
import sys
import socket
import time
import redis
from google.cloud import logging

logclient = logging.Client()
logger = logclient.logger( "ffmpeg-pool" )

PROJECT_ID = 'transcode-159215'
TOPIC = 'projects/{}/topics/message'.format(PROJECT_ID)
psclient = None
pstopic = None
pssub = None

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

def transcode( rfile ):
    client = storage.Client( PROJECT_ID )
    bucket = client.bucket( PROJECT_ID + ".appspot.com" )
    blob = bucket.blob( rfile )

    logger.log_text( "Worker: Downloaded part: " + rfile )

    with open( "/tmp/" + rfile, 'w' ) as f:
        blob.download_to_file( f )
    
    os.system( "rm /tmp/output*" )
    ret = os.system( "ffmpeg -i " + rfile + " -c:v libvpx -crf 10 -b:v 1M -c:a libvorbis /tmp/output-" + rfile )
    
    if ret:
        sys.stderr.write( "Failed" )
        return

    logger.log_text( "Worker: Uploaded part: output-" + rfile )
    blob = bucket.blob( "output-" + rfile )
    blob.upload_from_file( open( "/tmp/output-" + rfile ) )


def subscribe():
    global psclient, pstopic, pssub

    psclient = pubsub.Client( PROJECT_ID )
    pstopic = psclient.topic( "ffmpeg-pool" )

    if not pstopic.exists():
        pstopic.create()
    
    pssub = pstopic.subscription( "ffmpeg-worker-" + socket.gethostname() )
    #pssub = pubsub.Subscription( "ffmpeg-worker-" + socket.gethostname(), topic=pstopic )
    
    if not pssub.exists():
        pssub.create()
    

def handlemessages():
    global psclient, pstopic, pssub
    
    rqueue = RedisQueue('test')
    subscribe()

    while True:
        messages = pssub.pull( return_immediately=False, max_messages=110 )

        for ack_id, message in messages:
            logger.log_text( "Worker: Received message: " + message.data )
            
            try:
                #subscription.acknowledge( [ack_id] )
                pssub.acknowledge( [ack_id] )
                sys.stdout.write( "Processing: " + message.data )
                
                rfile = rqueue.get()
                logger.log_text( "Worker: Redis popped: " + rfile )

                while rfile != "None":
                    transcode( rfile )
                    rfile = rqueue.get()
                    logger.log_text( "Worker: Redis popped: " + rfile )

            except Exception as e:
                logger.log_text( "Worker: Error:" + e.message )
                sys.stderr.write( e.message )

        subscribe()
        time.sleep( 1 )


if __name__ == '__main__':
    handlemessages()
