import boto3

def extract_data(bucket_name, file_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, file_key, local_path)
    print(f"Downloaded {file_key} from {bucket_name} to {local_path}")
