import kagglehub as kh
import shutil
import os
import sys

def download_and_copy_dataset():
    try:
        print("Downloading dataset from Kaggle...")
        path = kh.dataset_download("olistbr/brazilian-ecommerce")
        destination_path = os.path.join(os.getcwd(), "Dataset_Raw")
        print(destination_path)
        shutil.copytree(path, destination_path, dirs_exist_ok=True)
        return f'Dataset downloaded and copied successfully in {destination_path}.'
    
    except Exception as e:
        print(f"Error occurred while downloading dataset: {e}")
        sys.exit(1)

if __name__ == "__main__":
    result = download_and_copy_dataset()
    print(result)