import os
from gcloud import storage, pubsub, logging
import sys
import socket
import time
import redis
import glob
from google.cloud import logging


logclient = logging.Client()
logger = logclient.logger( "ffmpeg-pool" )

PROJECT_ID = 'transcode-159215'
TOPIC = 'projects/{}/topics/message'.format(PROJECT_ID)
psclient = None
pstopic = None
pssub = None

class RedisQueue(object):
    def __init__( self, name, namespace = 'queue' ):
       self.__db = redis.Redis( host = "redis-11670.c10.us-east-1-4.ec2.cloud.redislabs.com", port=11670 )
       self.key = '%s:%s' %(namespace, name)

    def qsize( self ):
        return self.__db.llen( self.key )

    def empty( self ):
        return self.qsize() == 0

    def put( self, item ):
        self.__db.rpush( self.key, item )

    def get( self, block=True, timeout=None ):
        if block:
            item = self.__db.blpop( self.key, timeout=timeout )
        else:
            item = self.__db.lpop( self.key )

        if item:
            item = item[1]
        return item

    def get_nowait( self ):
        return self.get( False )


def download( rfile ):
    client = storage.Client( PROJECT_ID )
    bucket = client.bucket( PROJECT_ID + ".appspot.com" )
    blob = bucket.blob( rfile )

    with open( "/tmp/" + rfile, 'w' ) as f:
        blob.download_to_file( f )
        logger.log_text( "Worker: Downloaded: /tmp/" + rfile )


def upload( rfile ):
    client = storage.Client( PROJECT_ID )
    bucket = client.bucket( PROJECT_ID + ".appspot.com" )
    blob = bucket.blob( rfile )
    
    blob = bucket.blob( rfile )
    blob.upload_from_file( open( "/tmp/" + rfile ) )

    logger.log_text( "Worker: Uploaded /tmp/" + rfile )


def transcode( rfile ):
    download( rfile )
    
    os.system( "rm /tmp/output*" )
    ret = os.system( "ffmpeg -i /tmp/" + rfile + " -c:v libx265 -preset medium -crf 28 -c:a aac -b:a 128k -strict -2 /tmp/output-" + rfile + ".mkv" )    
    
    if ret:
        logger.log_text( "Worker: convert failed : " + rfile + " - " + str( ret ).encode( 'utf-8' ) )
        return

    upload( "output-" + rfile + ".mkv" ) 


def split():
    rqueue = RedisQueue( "test" )
    download( "sample.mp4" )

    os.system( "rm -f /tmp/chunk*" )
    ret = os.system( "ffmpeg -i /tmp/sample.mp4 -map 0:a -map 0:v -codec copy -f segment -segment_time 10 -segment_format matroska -v error '/tmp/chunk-%03d.orig'" )

    if ret:
        return "Failed"

    for rfile in glob.glob( "/tmp/chunk*" ):
        basename = os.path.basename( rfile )
        upload( basename )
        rqueue.put( basename )


def combine():
    client = storage.Client( PROJECT_ID )
    bucket = client.bucket( PROJECT_ID + ".appspot.com" )
    blobs = bucket.list_blobs()

    os.system( "rm /tmp/*" )
    
    names = []
    
    for blob in blobs:
        if "output" in blob.name:
            names.append( blob.name.encode( 'utf-8' ) )

    names.sort()

    with open( '/tmp/combine.lst', 'w' ) as f1:
        for name in names:
            f1.write( "file '/tmp/" + name + "'\n" )
            download( name )

    logger.log_text( "Worker: created combine list: /tmp/combine.lst" )

    ret = os.system( "ffmpeg -f concat -safe 0 -i  /tmp/combine.lst -c copy /tmp/combined.mkv" )    
    
    if ret:
        logger.log_text( "Worker: combine failed: /tmp/combine.mkv - " + str(ret).encode( 'utf-8' ) )
        return

    upload( "combined.mkv" )


def subscribe():
    global psclient, pstopic, pssub

    psclient = pubsub.Client( PROJECT_ID )
    pstopic = psclient.topic( "ffmpeg-pool" )

    if not pstopic.exists():
        pstopic.create()
    
    pssub = pstopic.subscription( "ffmpeg-worker-" + socket.gethostname() )
    
    if not pssub.exists():
        pssub.create()
    

def handlemessages():
    global psclient, pstopic, pssub
    
    rqueue = RedisQueue( 'test' )
    subscribe()

    while True:
        messages = pssub.pull( return_immediately=False, max_messages=110 )

        for ack_id, message in messages:
            payload = message.data.encode( 'utf-8' ).replace( u"\u2018", "'" ).replace( u"\u2019", "'" )
            logger.log_text( "Worker: Received message: " + payload )
 
            try:
                pssub.acknowledge( [ack_id] )
                if payload == "combine":
                    combine()
                elif payload  == "split":
                    split()
                else:
                    rfile = rqueue.get()
                    basename = os.path.basename( rfile )
                    logger.log_text( "Worker: Redis popped: " + basename )

                    while basename != "None":
                        transcode( basename )
                        rfile = rqueue.get()
                        basename = os.path.basename( rfile )
                        logger.log_text( "Worker: Redis popped: " + rfile )

            except Exception as e:
                logger.log_text( "Worker: Error: " + e.message )
                sys.stderr.write( e.message )

        subscribe()
        time.sleep( 1 )


if __name__ == '__main__':
    handlemessages()
