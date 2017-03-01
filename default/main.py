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


def publish( msg ):
    pubsub_client = pubsub.Client( PROJECT_ID )
    topic = pubsub_client.topic( "ffmpeg-pool" )

    if not topic.exists():
        topic.create()

    topic.publish( msg )


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

@app.route( "/split" )
def split():
    publish( "split" )
    return "File queued for spliting"


@app.route( "/transcode" )
def transcode():
    publish( "transcode" )
    return "Job queued for transcoding"


@app.route( "/combine" )
def combine():
    publish( "combine" )
    return "Job queued for combining"


@app.route( "/" )
def home():
    return "<a href='/split'>/split</a> | <a href='/transcode'>/transcode</a> | <a href='/combine'>/combine</a> | <a href='/cleantopic'>/cleantopic</a> | <a href='/readlog'>/readlog</a>"


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

