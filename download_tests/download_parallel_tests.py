import time
from google.cloud import storage
from datetime import datetime
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

TOKEN_PATH = "..."
client = storage.Client.from_service_account_json(TOKEN_PATH)

def download_with_gcloud_storage(gs_url, local_path):
    subprocess.run(["gcloud", "storage", "cp", gs_url, local_path])

def download_with_gsutil(gs_url, local_path):
    subprocess.run(["gsutil", "cp", gs_url, local_path])

def download_with_storage_client(bucket_name, blob_name, local_path):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)

def process_file(gs_url, download_folder, download_type):
    gs_url = gs_url.strip()
    if gs_url.startswith("gs://"):
        bucket_name, blob_name = gs_url.replace('gs://', '').split('/', 1)
        local_path = f"{download_folder}/{blob_name.split('/')[-1]}"

        if download_type == "storage_client":
            download_with_storage_client(bucket_name, blob_name, local_path)
        elif download_type == "gsutil":
            download_with_gsutil(gs_url, local_path)
        elif download_type == "gcloud_storage":
            download_with_gcloud_storage(gs_url, local_path)
        
        return local_path
    return None

def download_files_from_manifest(manifest_file, download_folder, log_folder, interval_log=10, interval_print=10, max_workers=10, download_type="storage_client"):
    with open(manifest_file, 'r') as f:
        lines = f.readlines()

    lines = lines[:1000]
    
    total_files = len(lines)
    start_time = time.time()
    log_file = Path(log_folder) / "download_progress.csv"
    error_log_file = Path(log_folder) / "download_error.txt"
    
    with open(log_file, 'w+') as log:
        log.write(f"time,num_file,rel_time\n")
    
    with open(error_log_file, 'w+') as log:
        log.write(f"file")


    # Monitorowanie postępu
    downloaded_files = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, line, download_folder, download_type): line for line in lines}

        for future in as_completed(futures):
            
            try:
                url = futures[future]
                
                future.result()
                elapsed_time = time.time() - start_time
                avg_time_per_file = elapsed_time / downloaded_files if downloaded_files > 0 else 0
                remaining_files = total_files - downloaded_files

                eta = remaining_files * avg_time_per_file
                
                if downloaded_files % interval_log == 0 or remaining_files==1:
                    relative_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
                    with open(log_file, 'a') as log:
                        log.write(f"{datetime.now()},{downloaded_files},{relative_time}\n")

                if downloaded_files % interval_print == 0 or remaining_files==1:
                    print(f"Downloaded {downloaded_files}/{total_files} files... | ETA: {time.strftime('%H:%M:%S', time.gmtime(eta))}, {round(eta/60/60, 2)} h | Elapsed: {relative_time}")

            except Exception as e:
                with open(error_log_file, 'a') as log:
                    log.write(f"{url}")


                print(f"Error downloading {e}")
            finally:
                downloaded_files += 1


    elapsed_time = time.time() - start_time
    relative_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print(f"Zakończono pobieranie {total_files} plików w czasie {relative_time}.")


downloads = [
    {
        "download_type": "gsutil",
        "workers_list": [1,2,4,8,10,12,14,18,24,32]
    }, 
    {
        "download_type": "storage_client",
        "workers_list": [1,2,4,8,10,12,14,18,24,32,48,64,80,96,128]
    }, 
    {
        "download_type": "gcloud_storage",
        "workers_list": [1,2,4,8,10,12,14,18,24,32]
    }
]

for download in downloads:
    for num_workers in download["workers_list"]:

        download_folder = './tmp'
        Path(download_folder).mkdir(exist_ok=True, parents=True)
        manifest_file = "./test_manifest.txt"
        log_folder = f"./{download["download_type"]}/results_{num_workers}_workers"
        Path(log_folder).mkdir(exist_ok=True, parents=True)

        download_files_from_manifest(
            manifest_file=manifest_file,
            download_folder=download_folder,
            log_folder=log_folder,
            interval_log=100,
            interval_print=100,
            max_workers=num_workers,
            download_type=download["download_type"]
        )
        
        shutil.rmtree(download_folder)