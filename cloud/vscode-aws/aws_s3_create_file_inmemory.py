import boto3
from io import BytesIO

# Create an S3 client
s3 = boto3.client('s3')

# Define bucket name and S3 object key
bucket_name = 'my-bucket-957122283754'
s3_object_key = 'folder/in/s3/in_memory_file.txt'

# Create a file-like object from a string
file_content = b"This is the content of my in-memory file."
in_memory_file = BytesIO(file_content)

try:
    s3.upload_fileobj(in_memory_file, bucket_name, s3_object_key)
    print(f"In-memory file uploaded successfully to '{bucket_name}/{s3_object_key}'")
except Exception as e:
    print(f"Error uploading in-memory file: {e}")