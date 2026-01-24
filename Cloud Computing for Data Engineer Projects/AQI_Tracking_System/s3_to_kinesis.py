import boto3
import json
import csv
from datetime import datetime
from botocore.exceptions import ClientError
from io import StringIO

# AWS Configuration
S3_BUCKET = 'aqidatafromindia'
S3_KEY_PATTERN = 'openaq_location_'  # Pattern to match files
KINESIS_STREAM = 'AQI_Stream'
AWS_REGION = 'us-east-1'  

def get_aws_session():
    """Create AWS session with explicit credentials"""
    session = boto3.Session(
        region_name=AWS_REGION,
        aws_access_key_id='***',    # Replace with your key
        aws_secret_access_key='**' # Replace with your secret
    )
    return session

def list_matching_files(session, bucket, prefix):
    """List all files matching the pattern in S3 bucket"""
    s3 = session.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            print(f" No files found matching pattern: {prefix}* in bucket {bucket}")
            return None
        file_keys = []
        for obj in response['Contents']:
            file_keys.append(obj['Key'])
            print(obj['Key']+'=keys')
        matching_files = sorted(file_keys)
        print(matching_files)
        print(f" Found {len(matching_files)} matching files in S3")
        return matching_files
    except ClientError as e:
        print(f" Error listing S3 objects: {str(e)}")
        return None

def read_csv_from_s3(session, bucket, key):
    """Read CSV file from S3 and return as list of dictionaries in original order"""
    s3 = session.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Parse CSV 
        reader = csv.DictReader(StringIO(csv_content))
        records = []
        for row in reader:
            # Convert each row to a properly formatted dictionary
            formatted_record = {
                'location_id': row.get('location_id', ''),
                'location_name': row.get('location_name', ''),
                'parameter_of_AQI': row.get('parameter_of_AQI', ''),
                'value_of_AQI': row.get('value_of_AQI', ''),
                'unit': row.get('unit', ''),
                'datetimeUtc': row.get('datetimeUtc', ''),
                'datetimeLocal': row.get('datetimeLocal', ''),
                'timezone': row.get('timezone', ''),
                'latitude': row.get('latitude', ''),
                'longitude': row.get('longitude', ''),
                'country_iso': row.get('country_iso', ''),
                'isMobile': row.get('isMobile', ''),
                'isMonitor': row.get('isMonitor', ''),
                'owner_name': row.get('owner_name', ''),
                'provider': row.get('provider', '')
            }
            records.append(formatted_record)
        
        print(f" Read {len(records)} records from {key}")
        return records
    except ClientError as e:
        print(f" Error reading {key}: {str(e)}")
        return None
    except Exception as e:
        print(f" Unexpected error processing {key}: {str(e)}")
        return None

def send_to_kinesis(session, records, sequence_token=None):
    """Send records to Kinesis in sequence with error handling"""
    kinesis = session.client('kinesis')
    success_count = 0
    sequence_number = sequence_token
    
    for i, record in enumerate(records, 1):
        try:
            # Use location_id as partition key to keep related data together
            partition_key = record['location_id']
            
            put_args = {
                'StreamName': KINESIS_STREAM,
                'Data': json.dumps(record),
                'PartitionKey': partition_key
            }
            
            # If we have a sequence number from previous puts, use it
            if sequence_number:
                put_args['SequenceNumberForOrdering'] = sequence_number
            
            response = kinesis.put_record(**put_args)
            sequence_number = response['SequenceNumber']
            success_count += 1
            
            if i % 100 == 0 or i == len(records):
                print(f"📨 Sent {i}/{len(records)} records (Last Seq: {sequence_number[:10]}...)")
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print(" Access Denied to Kinesis. Please check:")
                print(f"- Stream name: {KINESIS_STREAM}")
                print(f"- IAM permissions: kinesis:PutRecord")
                break
            else:
                print(f" Kinesis Error on record {i}: {str(e)}")
        except Exception as e:
            print(f" Failed to send record {i}: {str(e)}")
    
    return success_count, sequence_number

def process_all_files(session):
    """Process all matching CSV files in order"""
    # List all matching files in sorted order
    files = list_matching_files(session, S3_BUCKET, S3_KEY_PATTERN)
    if not files:
        return 0, 0
    
    total_records = 0
    total_success = 0
    sequence_token = None
    
    for file_key in files:
        print(f"\nProcessing file: {file_key}")
        records = read_csv_from_s3(session, S3_BUCKET, file_key)
        if not records:
            continue
            
        # Send records with sequence token for ordering
        success_count, sequence_token = send_to_kinesis(session, records, sequence_token)
        total_records += len(records)
        total_success += success_count
        
        # If we failed to send any records, stop processing
        if success_count < len(records):
            break
    
    return total_records, total_success

def main():
    print(f"Starting process at {datetime.now().isoformat()}")
    print(f"Reading from s3://{S3_BUCKET}/{S3_KEY_PATTERN}*")
    
    # Create authenticated session
    session = get_aws_session()
    
    # Process all matching files
    total_records, total_success = process_all_files(session)
    
    # Results
    print("\n🔍 Processing complete")
    print(f"Total files processed: {len(files) if 'files' in locals() else 0}")
    print(f"Total records: {total_records}")
    print(f"Successfully sent: {total_success}")
    print(f"Failed: {total_records - total_success}")

if __name__ == "__main__":
    main()