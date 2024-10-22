import boto3
import json
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

# Initialize Textract client
textract = boto3.client('textract', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

#Initialise the List
all_extracted_text = []

# Define a function to process a single PDF
def process_pdf(pdf_key, source_bucket, destination_bucket, destination_folder):
    try:
        # Use Textract to extract text content from PDF
        response = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': source_bucket, 'Name': pdf_key}}
        )
        job_id = response['JobId']

        # Wait for Textract analysis to complete
        textract_result = None
        while True:
            response = textract.get_document_text_detection(JobId=job_id)
            if response['JobStatus'] == 'SUCCEEDED':
                textract_result = response
                break
        
        # Process Textract result and create the desired output format
        extracted_text = []
        for item in textract_result['Blocks']:
            if item['BlockType'] == 'LINE':
                extracted_text.append({
                    "id": item['Id'],
                    "text": item['Text'],
                    "timestamp": 0,  # You can adjust this as needed
                    "user": "",
                    "is_section_header": False,  # You can adjust this as needed
                    "awry_info": {}
                })

        # Convert extracted text to JSON
        all_extracted_text.extend(extracted_text)

        logger.info(f"Processed: {pdf_key}")
    except Exception as e:
        logger.error(f"Error processing {pdf_key}: {e}")

# Main function
def main(s3_client):
    source_bucket = 'taoai-gboluwaga-s3bucket'
    source_folder = 'async-doc-text'
    destination_bucket = 'taoai-gboluwaga-s3bucket'
    destination_folder = 'json'

    # List objects in the source folder
    response = s3_client.list_objects(Bucket=source_bucket, Prefix=source_folder)
    objects = response.get('Contents', [])

    all_extracted_text = []

    # Create a ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        for obj in objects:
            object_key = obj['Key']
            if object_key.endswith('.pdf'):
                executor.submit(process_pdf, object_key, source_bucket, destination_bucket, destination_folder)
    
    # Create a DataFrame from the extracted text list
    df = pd.DataFrame(all_extracted_text)

    # Convert the DataFrame to a JSON file
    json_data = df.to_json(orient='records')

    # Save JSON to destination bucket folder
    json_key = f"{destination_folder}/extracted_text.json"
    s3.put_object(Bucket=destination_bucket, Key=json_key, Body=json_data)

if __name__ == '__main__':
    main(s3)

