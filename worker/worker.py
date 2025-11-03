#!/usr/bin/env python3

import sys
import os

# Immediate startup message
print("WORKER STARTING - INITIAL CHECK", flush=True)

try:
    import redis
    print("✓ Redis imported", flush=True)
except Exception as e:
    print(f"✗ Failed to import redis: {e}", flush=True)
    sys.exit(1)

try:
    import jsonpickle
    print("✓ jsonpickle imported", flush=True)
except Exception as e:
    print(f"✗ Failed to import jsonpickle: {e}", flush=True)
    sys.exit(1)

try:
    import platform
    import requests
    from minio import Minio
    from minio.error import S3Error
    print("✓ All imports successful", flush=True)
except Exception as e:
    print(f"✗ Failed to import dependencies: {e}", flush=True)
    sys.exit(1)

# Configuration
redisHost = os.getenv("REDIS_HOST", "localhost")
redisPort = int(os.getenv("REDIS_PORT", 6379))
minioHost = os.getenv("MINIO_HOST", "localhost:9000")
minioUser = os.getenv("MINIO_USER", "rootuser")
minioPwd = os.getenv("MINIO_PASS", "rootpass123")

print(f"Config: Redis={redisHost}:{redisPort}, MinIO={minioHost}", flush=True)

# Initialize clients
try:
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
    print("✓ Redis client created", flush=True)
except Exception as e:
    print(f"✗ Failed to create Redis client: {e}", flush=True)
    sys.exit(1)

try:
    minioClient = Minio(minioHost, access_key=minioUser, secret_key=minioPwd, secure=False)
    print("✓ MinIO client created", flush=True)
except Exception as e:
    print(f"✗ Failed to create MinIO client: {e}", flush=True)
    sys.exit(1)

# Logging
infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"

def log_debug(message):
    print(f"DEBUG: {message}", file=sys.stdout, flush=True)
    try:
        redisClient.lpush('logging', f"{debugKey}:{message}")
    except Exception as e:
        print(f"Failed to log to Redis: {e}", file=sys.stderr, flush=True)

def log_info(message):
    print(f"INFO: {message}", file=sys.stdout, flush=True)
    try:
        redisClient.lpush('logging', f"{infoKey}:{message}")
    except Exception as e:
        print(f"Failed to log to Redis: {e}", file=sys.stderr, flush=True)

def download_from_minio(bucket, object_name, file_path):
    try:
        log_debug(f"Downloading {object_name} from bucket {bucket}")
        minioClient.fget_object(bucket, object_name, file_path)
        log_debug(f"Successfully downloaded to {file_path}")
        return True
    except S3Error as e:
        log_debug(f"Error downloading from MinIO: {str(e)}")
        return False

def upload_to_minio(bucket, object_name, file_path):
    try:
        log_debug(f"Uploading {file_path} to bucket {bucket} as {object_name}")
        minioClient.fput_object(bucket, object_name, file_path, content_type='audio/mpeg')
        log_info(f"Successfully uploaded {object_name}")
        return True
    except S3Error as e:
        log_debug(f"Error uploading to MinIO: {str(e)}")
        return False

def separate_audio(songhash, model='htdemucs'):
    log_info(f"Starting separation for {songhash} with model {model}")
    
    input_dir = "/tmp/input"
    output_dir = "/tmp/output"
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    input_file = f"{input_dir}/{songhash}.mp3"
    if not download_from_minio('queue', f"{songhash}.mp3", input_file):
        log_debug(f"Failed to download {songhash}.mp3 from MinIO")
        return False
    
    log_info(f"Running DEMUCS separation on {songhash}")
    
    # Just use the simplest command
    demucs_cmd = f"python3 -m demucs.separate --mp3 --out {output_dir} {input_file}"
    
    log_debug(f"Executing: {demucs_cmd}")
    exit_code = os.system(demucs_cmd)
    
    if exit_code != 0:
        log_debug(f"DEMUCS failed with exit code {exit_code}")
        return False
    
    log_info(f"DEMUCS separation completed for {songhash}")
    
    # Find the actual output directory (could be htdemucs, mdx_extra, mdx_extra_q, etc.)
    import glob
    possible_dirs = glob.glob(f"{output_dir}/*/{songhash}")
    
    if not possible_dirs:
        log_debug(f"No output directory found for {songhash}")
        return False
    
    demucs_output_dir = possible_dirs[0]
    log_debug(f"Found output directory: {demucs_output_dir}")
    
    # Default outputs 4 tracks
    tracks = ['bass.mp3', 'drums.mp3', 'vocals.mp3', 'other.mp3']
    upload_success = True
    
    for track in tracks:
        track_path = f"{demucs_output_dir}/{track}"
        if not os.path.exists(track_path):
            log_debug(f"Track file not found: {track_path}")
            upload_success = False
            continue
        
        object_name = f"{songhash}-{track}"
        if not upload_to_minio('output', object_name, track_path):
            upload_success = False
    
    try:
        os.system(f"rm -rf {input_dir}/{songhash}.mp3")
        os.system(f"rm -rf {demucs_output_dir}")
        log_debug(f"Cleaned up temporary files for {songhash}")
    except Exception as e:
        log_debug(f"Error cleaning up: {str(e)}")
    
    return upload_success

def send_callback(callback_url, songhash):
    try:
        log_debug(f"Sending callback to {callback_url}")
        payload = {'hash': songhash, 'status': 'completed'}
        response = requests.post(callback_url, json=payload, timeout=5)
        log_info(f"Callback sent, status code: {response.status_code}")
    except Exception as e:
        log_debug(f"Callback failed: {str(e)}")

def process_work_item(work_item):
    try:
        songhash = work_item.get('hash')
        model = work_item.get('model', 'htdemucs')
        callback = work_item.get('callback')
        
        log_info(f"Processing work item: {songhash}")
        success = separate_audio(songhash, model)
        
        if success:
            log_info(f"Successfully processed {songhash}")
            if callback:
                send_callback(callback, songhash)
        else:
            log_debug(f"Failed to process {songhash}")
        
        return success
    except Exception as e:
        log_debug(f"Error processing work item: {str(e)}")
        return False

def main():
    print("=== WORKER MAIN FUNCTION STARTED ===", flush=True)
    
    try:
        log_info("Worker starting...")
        log_info(f"Connecting to Redis at {redisHost}:{redisPort}")
        log_info(f"Connecting to MinIO at {minioHost}")
        
        # Test connections
        try:
            redisClient.ping()
            log_info("✓ Redis connection successful")
        except Exception as e:
            log_info(f"✗ Redis connection failed: {e}")
            print(f"Redis connection failed: {e}", flush=True)
        
        try:
            if not minioClient.bucket_exists('queue'):
                log_info("Creating 'queue' bucket")
                minioClient.make_bucket('queue')
            if not minioClient.bucket_exists('output'):
                log_info("Creating 'output' bucket")
                minioClient.make_bucket('output')
            log_info("✓ MinIO connection successful")
        except Exception as e:
            log_debug(f"Error checking/creating buckets: {str(e)}")
        
        log_info("Worker ready, waiting for work...")
        
        while True:
            try:
                work = redisClient.blpop('toWorker', timeout=0)
                if work:
                    work_data = work[1]
                    log_debug(f"Received work item: {work_data}")
                    work_item = jsonpickle.decode(work_data)
                    process_work_item(work_item)
            except KeyboardInterrupt:
                log_info("Worker shutting down...")
                break
            except Exception as e:
                log_debug(f"Error in main loop: {str(e)}")
                continue
            
            sys.stdout.flush()
            sys.stderr.flush()
            
    except Exception as e:
        print(f"FATAL ERROR in main: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    print("=== WORKER SCRIPT STARTING ===", flush=True)
    main()