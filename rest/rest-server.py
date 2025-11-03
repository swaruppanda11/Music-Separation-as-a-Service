#!/usr/bin/env python3

from flask import Flask, request, jsonify, send_file
import redis
import hashlib
import base64
import jsonpickle
import io
import os
import sys
import platform
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

# Configuration
redisHost = os.getenv("REDIS_HOST", "localhost")
redisPort = int(os.getenv("REDIS_PORT", 6379))
minioHost = os.getenv("MINIO_HOST", "localhost:9000")
minioUser = os.getenv("MINIO_USER", "rootuser")
minioPwd = os.getenv("MINIO_PASS", "rootpass123")

# Initialize clients
redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
minioClient = Minio(minioHost, access_key=minioUser, secret_key=minioPwd, secure=False)

# Logging
infoKey = f"{platform.node()}.rest.info"
debugKey = f"{platform.node()}.rest.debug"

def log_debug(message):
    print(f"DEBUG: {message}", file=sys.stdout, flush=True)
    try:
        redisClient.lpush('logging', f"{debugKey}:{message}")
    except Exception as e:
        print(f"Failed to log to Redis: {e}", file=sys.stderr)

def log_info(message):
    print(f"INFO: {message}", file=sys.stdout, flush=True)
    try:
        redisClient.lpush('logging', f"{infoKey}:{message}")
    except Exception as e:
        print(f"Failed to log to Redis: {e}", file=sys.stderr)

def ensure_buckets():
    buckets = ['queue', 'output']
    for bucket in buckets:
        try:
            if not minioClient.bucket_exists(bucket):
                minioClient.make_bucket(bucket)
                log_info(f"Created bucket: {bucket}")
        except S3Error as e:
            log_debug(f"Error ensuring bucket {bucket}: {str(e)}")

@app.route('/', methods=['GET'])
def hello():
    return '<h1>Music Separation Server</h1><p>Use a valid endpoint</p>'

@app.route('/apiv1/separate', methods=['POST'])
def separate():
    try:
        data = request.get_json()
        if not data or 'mp3' not in data:
            return jsonify({'error': 'Missing mp3 data in request'}), 400
        
        mp3_data = base64.b64decode(data['mp3'])
        model = data.get('model', 'htdemucs')
        callback = data.get('callback', None)
        songhash = hashlib.sha256(mp3_data).hexdigest()[:56]
        
        log_info(f"Processing separation request for hash: {songhash}")
        
        mp3_stream = io.BytesIO(mp3_data)
        minioClient.put_object('queue', f"{songhash}.mp3", mp3_stream, len(mp3_data), content_type='audio/mpeg')
        log_debug(f"Stored MP3 in MinIO: {songhash}.mp3")
        
        work_item = {'hash': songhash, 'model': model, 'callback': callback}
        redisClient.lpush('toWorker', jsonpickle.encode(work_item))
        log_info(f"Queued song {songhash} for separation with model {model}")
        
        return jsonify({'hash': songhash, 'reason': 'Song enqueued for separation'}), 200
    except Exception as e:
        log_debug(f"Error in /apiv1/separate: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        queue_items = redisClient.lrange('toWorker', 0, -1)
        hashes = []
        for item in queue_items:
            try:
                work_item = jsonpickle.decode(item)
                hashes.append(work_item['hash'])
            except:
                pass
        log_debug(f"Queue status requested. Current queue size: {len(hashes)}")
        return jsonify({'queue': hashes}), 200
    except Exception as e:
        log_debug(f"Error in /apiv1/queue: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def get_track(songhash, track):
    try:
        object_name = f"{songhash}-{track}"
        log_debug(f"Retrieving track: {object_name}")
        response = minioClient.get_object('output', object_name)
        mp3_data = response.read()
        response.close()
        response.release_conn()
        log_info(f"Successfully retrieved track: {object_name}")
        return send_file(io.BytesIO(mp3_data), mimetype='audio/mpeg', as_attachment=True, download_name=track)
    except S3Error as e:
        log_debug(f"Track not found in MinIO: {object_name}")
        return jsonify({'error': 'Track not found'}), 404
    except Exception as e:
        log_debug(f"Error retrieving track: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/apiv1/remove/<songhash>/<track>', methods=['GET'])
def remove_track(songhash, track):
    try:
        object_name = f"{songhash}-{track}"
        minioClient.remove_object('output', object_name)
        log_info(f"Removed track: {object_name}")
        return jsonify({'status': 'success', 'message': f'Removed {object_name}'}), 200
    except Exception as e:
        log_debug(f"Error in remove: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    log_info("REST server starting...")
    ensure_buckets()
    log_info("REST server ready on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False)