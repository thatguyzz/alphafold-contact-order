# alphafold-contact-order

Project for Large-Scale Computing (LSC) classes focused on processing AlphaFold data

### Prerequisites

1. **Google Storage Utilities**:  
   Install `gsutil` from the [Google Cloud documentation](https://cloud.google.com/storage/docs/gsutil_install).

2. **Google Cloud CLI**:  
   Install the `gcloud` command-line tool following the [installation guide](https://cloud.google.com/sdk/docs/install).  
   - **Note**: Both `gsutil` and `gcloud` require initialization. Log in with your Google credentials to configure them.

3. **Python Environment and Required Packages**:  
   Set up the Python environment and install the necessary packages by running:  
   ```bash
   pip install -r requirements.txt
   ```

4. **Google CLI Token**:  
   To use the Python library, generate a Google CLI token and ensure it is configured properly.


### Conclusions

1. **File limit on PLGrid `$SCRATCH` (ares)**: The maximum number of files is 1 million.  
2. **User authentication**:  
   - The dataset requires a logged-in Google user.  
   - There were issues with authentication when using tools like `wget`, which complicated its usage.  
3. **Google Cloud and API**:  
   - Each method required creating a project in Google Cloud and obtaining an API key.  
4. **Batch data processing**:  
   - A batch solution was implemented: downloading, processing, and deleting files in a single batch. 
   - However, this approach was not fully utilized due to the client object’s non-serializability.
5. **PLGrid infrastructure**:  
   - Tools like `gsutil` and `gcloud storage` are not available in the PLGrid infrastructure, so tests were conducted locally.  
6. **Scaling and parallelism**:  
   - Contact order is calculated independently for each thread, allowing linear scaling.  
   - The focus was on studying limitations related to parallel file downloading.  
7. **Data compression and decompression**:  
   - `gsutil` and `gcloud storage` strained the system due to decompression, which extended processing time.  
   - It seems, that the Python library also applied compression, but the system overhead was significantly lower.  
   - It’s possible that optimizing `gsutil` and `gcloud storage` would require using file manifests, but this solution was not tested.  
8. **Hardware configuration**:  
   - Tests were conducted on a 6C/12T processor.  
   - `ThreadPool` parameters (from the `concurrent` library) were adjusted for the low CPU load during file downloading.  
9. **Download stability**:  
   - The Python library encountered errors during file downloads (on average ~6 % of files).  
   - All files were successfully downloaded using `gsutil` and `gcloud storage`.
10. **Performance Observation**:  
   - The Python library was significantly faster than other methods.  
   - The likely reason for this improvement is the bottleneck caused by decompression in other tools, such as `gsutil` and `gcloud`.