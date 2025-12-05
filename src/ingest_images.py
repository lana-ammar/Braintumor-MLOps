import os
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Config (replace with your values)
STORAGE_ACCOUNT = "braintumor60107216"
CONTAINER_NAME = "raw"
LOCAL_DATA_PATH = "./data/brain_tumor_dataset"  # Adjust if needed
ADLS_BASE_PATH = "tumor_images"

def upload_to_adls(local_path, remote_path):
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net", credential=credential)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    for root, _, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, LOCAL_DATA_PATH)
            blob_path = f"{ADLS_BASE_PATH}/{relative_path}"
            blob_client = container_client.get_blob_client(blob_path)
            
            # Idempotent: Check if exists
            if not blob_client.exists():
                with open(local_file, "rb") as data:
                    blob_client.upload_blob(data)
                print(f"Uploaded: {blob_path}")
            else:
                print(f"Skipped (exists): {blob_path}")

if __name__ == "__main__":
    upload_to_adls(f"{LOCAL_DATA_PATH}/yes", "yes")
    upload_to_adls(f"{LOCAL_DATA_PATH}/no", "no")