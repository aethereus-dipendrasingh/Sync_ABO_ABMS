from simple_salesforce import Salesforce
from datetime import datetime
from functools import lru_cache
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from typing import Dict
from io import StringIO
import pandas as pd
import requests
import logging
import json
import html
import time
import os
import io

# for email sending
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# for web services
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

SF_API_VERSION = os.getenv('SF_API_VERSION', '58.0') # Use a recent API version

INSTANCE_URL = ''
ACCESS_TOKEN = ''

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging to only output to terminal
def configure_logging():
    """Configure logging to output to terminal only."""
    logger = logging.getLogger("salesforce_xml_api")
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = configure_logging()
load_dotenv(dotenv_path="creds.env")

class SalesforceAPIError(Exception):
    """Custom exception for Salesforce API errors."""
    def __init__(self, message, status_code=500, detail=None):
        self.message = message
        self.status_code = status_code
        self.detail = detail or message
        super().__init__(self.message)

@lru_cache(maxsize=1)
def get_salesforce_connection():
    """Create and cache Salesforce connection."""
    global INSTANCE_URL, ACCESS_TOKEN
    logger.info("Establishing Salesforce connection")
    try:
        sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_SECURITY_TOKEN"),
            domain=os.getenv("SF_DOMAIN"),
        )
        ACCESS_TOKEN = sf.session_id
        INSTANCE_URL = f"https://{sf.sf_instance}"
        logger.info("Salesforce connection established successfully")
        return sf
    except Exception as e:
        error_msg = f"Failed to connect to Salesforce: {str(e)}"
        logger.error(error_msg)
        raise SalesforceAPIError(error_msg, status_code=500)

def convert_rich_text_to_plain(html_content):
    """
    Convert rich text HTML to plain text with simplified formatting.
    
    Args:
        html_content (str): HTML content to parse
        
    Returns:
        str: Plain text extracted from HTML
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, "html.parser")
    lines = [
        html.unescape(paragraph.get_text()).replace("\xa0", " ") 
        for paragraph in soup.find_all("p")
    ]
    return "\n".join(lines).strip()

def get_salesforce_file(sf,query,file_type, is_csv):
    """Fetch data from Salesforce using a SOQL query."""
    logger.info(f"Executing query: {query}")
    try:
        result = sf.query(query)
        logger.info(f"Query executed successfully, found {result.get('totalSize', 0)} records")

        if result['totalSize'] > 0:
            record = result['records'][0]
            version_data_url = record['VersionDataUrl']
            
            # Prepare headers for the download request
            headers = {
                'Authorization': f'Bearer {sf.session_id}',
                'Content-Type': 'application/json'
            }
            
            try:
                # Download the CSV file
                response = requests.get(version_data_url, headers=headers)
                response.raise_for_status()  # Raise an error for bad status codes
                
                # Read the CSV content into a pandas DataFrame
                logger.info("Content-Type: "+str(response.headers.get('Content-Type')))
                logger.info("CSV file downloaded successfully"+response.text)
                if(is_csv):
                    # Read the CSV content into a pandas DataFrame
                    csv_content = StringIO(response.text)
                    df = pd.read_csv(csv_content, on_bad_lines='warn')
                else:
                    csv_content = StringIO(response.text)
                    if(file_type == 'LIDS'):
                        df = pd.read_csv(csv_content, delimiter="\t", on_bad_lines='warn')
                    else:
                        df = pd.read_csv(csv_content, delimiter=",", on_bad_lines='warn')
                    logger.info(df.head())
                
                # Display the table with headers
                logger.info("CSV file successfully downloaded and converted to table:")
                # logger.info(df)

                query = f"SELECT Id,DANS_Candidates_Field_Mapping__c, DANS_Diplomates_Field_Mapping__c,  LIDS_All_Active_Field_Mapping__c FROM ABOP_Migration__c WHERE Is_Active__c =true AND XML_Type__c = '{file_type}'"
                sfFieldMapping = sf.query(query)
                logger.info(f"Query executed successfully, found {sfFieldMapping.get('totalSize', 0)} records")
                if sfFieldMapping['totalSize'] > 0:
                    # Process the field mapping
                    for record in sfFieldMapping['records']:
                        # Extract field mappings
                        dans_candidates_mapping = record.get('DANS_Candidates_Field_Mapping__c')
                        dans_diplomates_mapping = record.get('DANS_Diplomates_Field_Mapping__c')
                        lids_all_active_mapping = record.get('LIDS_All_Active_Field_Mapping__c')

                        # Convert rich text to plain text
                        if dans_candidates_mapping:
                            dans_candidates_mapping = json.loads(convert_rich_text_to_plain(dans_candidates_mapping))
                        if dans_diplomates_mapping:
                            dans_diplomates_mapping = json.loads(convert_rich_text_to_plain(dans_diplomates_mapping))
                        if lids_all_active_mapping:
                            lids_all_active_mapping = json.loads(convert_rich_text_to_plain(lids_all_active_mapping))

                        # logger.info the mappings
                        # logger.info(f"DANS Candidates Mapping: {dans_candidates_mapping}")
                        # logger.info(f"DANS Diplomates Mapping: {dans_diplomates_mapping}")
                        # logger.info(f"LIDS All Active Mapping: {lids_all_active_mapping}")

                        if lids_all_active_mapping:
                            return df, lids_all_active_mapping
                        else:
                            return df, dans_candidates_mapping, dans_diplomates_mapping
                
            except requests.exceptions.RequestException as e:
                logger.info(f"Error downloading the file: {e}")
        else:
            logger.info("No ContentVersion record found with the specified title.")
            raise Exception("Sample.csv not found.")
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}"
        logger.error(error_msg)
        raise SalesforceAPIError(error_msg, status_code=500)    

def get_state_code_mapping(sf):
    """
    Query State_Code_Mapping__mdt and return a dict with StateCode as key and State as value.
    
    Args:
        sf: Salesforce connection object
        
    Returns:
        dict: Mapping of state names to state codes
    """
    logger.info("Querying State_Code_Mapping__mdt records")
    
    try:
        # Query all records from the custom metadata type
        query = """
            SELECT Label, State__c, Country__c 
            FROM State_Code_Mapping__mdt
            WHERE Country__c != NULL 
            AND State__c != NULL
        """
        
        result = sf.query(query)
        
        # Create a dictionary with State as key and StateCode as value
        code_to_state_map = {}
        code_to_country_map = {}
        
        if result and 'records' in result:
            for record in result['records']:
                state_code = record.get('Label')
                state = record.get('State__c')
                country = record.get('Country__c')
                
                if state_code and state and country:
                    code_to_state_map[state_code] = state
                    code_to_country_map[state_code] = country
        
        logger.info(f"Retrieved {len(code_to_state_map)} state-code mappings")
        return code_to_state_map, code_to_country_map
        
    except Exception as e:
        logger.error(f"Error querying State_Code_Mapping__mdt: {str(e)}", exc_info=True)
        return {}



    """
    Post a batch of records to Salesforce Composite API.

    Parameters:
        payload (Dict): JSON payload structured for Salesforce Composite API.
        instance_url (str): Salesforce instance URL.
        headers (Dict): Headers for the request.

    Returns:
        List[requests.Response]: List of responses from the API.
    """
    response = requests.post(instance_url, headers=headers, data=json.dumps(payload))
    logger.info(f"Posting batch to {instance_url} with payload: {json.dumps(payload, indent=4)}")
    logger.info(f"Response: {response.json()}")

    if response.status_code != 200:
        logger.error(f"Failed to post batch: {response.text}", exc_info=True)
        # raise SalesforceAPIError(f"Failed to post batch: {response.text}", status_code=response.status_code)

    return response.json().get("results", [])

def create_bulk_job(object_name, operation, external_id_field_name=None):
    """Creates a Bulk API 2.0 job."""
    logger.info(f"Creating Bulk API 2.0 job for {object_name}, operation: {operation}...")
    url = f"{INSTANCE_URL}/services/data/v{SF_API_VERSION}/jobs/ingest"
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json; charset=UTF-8',
        'Accept': 'application/json'
    }
    payload = {
        'object': object_name,
        'operation': operation, # 'insert', 'upsert', 'update', 'delete'
        'contentType': 'CSV',
        'lineEnding': 'LF' # Or 'CRLF'
    }
    if operation == 'upsert' and external_id_field_name:
        payload['externalIdFieldName'] = external_id_field_name

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        job_info = response.json()
        logger.info(f"Job created successfully. Job ID: {job_info['id']}")
        return job_info
    except requests.exceptions.HTTPError as e:
        logger.info(f"Error creating job for {object_name}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.info(f"Error creating job for {object_name}: {e}")
    return None

def upload_job_data(job_id, csv_data):
    """Uploads CSV data to the created Bulk API 2.0 job."""
    logger.info(f"Uploading data to job ID: {job_id}...")
    # The job_info from create_bulk_job contains a 'contentUrl' which is deprecated.
    # The new endpoint is jobs/ingest/<jobId>/batches
    url = f"{INSTANCE_URL}/services/data/v{SF_API_VERSION}/jobs/ingest/{job_id}/batches"
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'text/csv',
        'Accept': 'application/json'
    }
    try:
        response = requests.put(url, headers=headers, data=csv_data.encode('utf-8')) # Ensure UTF-8 encoding
        response.raise_for_status()
        logger.info(f"Data uploaded successfully to job {job_id}. Status: {response.status_code}") # Should be 201 Created
        return True
    except requests.exceptions.HTTPError as e:
        logger.info(f"Error uploading data for job {job_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.info(f"Error uploading data for job {job_id}: {e}")
    return False

def close_job(job_id):
    """Closes the Bulk API 2.0 job to start processing."""
    logger.info(f"Closing job ID: {job_id} to start processing...")
    url = f"{INSTANCE_URL}/services/data/v{SF_API_VERSION}/jobs/ingest/{job_id}"
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json; charset=UTF-8',
        'Accept': 'application/json'
    }
    payload = {'state': 'UploadComplete'}
    try:
        response = requests.patch(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        job_info = response.json()
        logger.info(f"Job {job_id} closed successfully. Current state: {job_info.get('state')}")
        return job_info
    except requests.exceptions.HTTPError as e:
        logger.info(f"Error closing job {job_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.info(f"Error closing job {job_id}: {e}")
    return None

def monitor_job_status(job_id, poll_interval=10, timeout_seconds=600):
    """Monitors the job status until it's completed or failed."""
    logger.info(f"Monitoring job ID: {job_id}...")
    url = f"{INSTANCE_URL}/services/data/v{SF_API_VERSION}/jobs/ingest/{job_id}"
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'application/json'}
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout_seconds:
            logger.info(f"Job {job_id} monitoring timed out after {timeout_seconds} seconds.")
            return None

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            job_info = response.json()
            state = job_info.get('state')
            logger.info(f"Job {job_id} status: {state}")

            if state in ['JobComplete', 'Failed', 'Aborted']:
                return job_info
        except requests.exceptions.HTTPError as e:
            logger.info(f"Error fetching job status for {job_id}: {e.response.status_code} - {e.response.text}")
            return None # Or retry logic
        except Exception as e:
            logger.info(f"Error fetching job status for {job_id}: {e}")
            return None

        time.sleep(poll_interval)

def get_job_results(job_id, result_type="successfulResults"):
    """Fetches successful or failed results for a completed job."""
    # result_type can be "successfulResults", "failedResults", "unprocessedrecords"
    logger.info(f"Fetching {result_type} for job ID: {job_id}...")
    url = f"{INSTANCE_URL}/services/data/v{SF_API_VERSION}/jobs/ingest/{job_id}/{result_type}/"
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'text/csv'} # Results are CSV

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # The response is CSV data
        logger.info(f"\n--- {result_type.capitalize()} for Job {job_id} ---")
        logger.info(response.text)
        logger.info("-------------------------------------\n")
        return response.text
    except requests.exceptions.HTTPError as e:
        logger.info(f"Error fetching {result_type} for job {job_id}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.info(f"Error fetching {result_type} for job {job_id}: {e}")
    return None

def send_email_with_attachments(subject, html_body, attachments=None):
    """
    Sends an email with HTML body and optional CSV attachments.
    attachments: list of tuples (filename, csv_data_string)
    """
    smtp_config = {
        "host": "smtp.gmail.com",
        "port": 587,
        "username": "aethereus12@gmail.com",
        "password": os.getenv("GMAIL_APP_PASSWORD"),
        "sender_email": "aethereus12@gmail.com",
        "receiver_email": "aethereus12@gmail.com"
    }
    SMTP_SERVER = smtp_config.get("host")
    SMTP_PORT = smtp_config.get("port")
    SMTP_USERNAME = smtp_config.get("username")
    SMTP_PASSWORD = smtp_config.get("password")
    EMAIL_SENDER = smtp_config.get("sender_email")
    EMAIL_RECEIVER = smtp_config.get("receiver_email")
    
    if not all([SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, EMAIL_SENDER, EMAIL_RECEIVER]):
        print("SMTP configuration is missing in environment variables. Skipping email notification.")
        return

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER # Can be a comma-separated list for multiple recipients

    # Attach HTML body
    part_html = MIMEText(html_body, 'html', 'utf-8')
    msg.attach(part_html)

    if attachments:
        for filename, csv_data in attachments:
            if csv_data: # Only attach if there's data
                part_csv = MIMEApplication(csv_data.encode('utf-8'), Name=filename)
                part_csv['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part_csv)
                print(f"Prepared attachment: {filename}")

    try:
        print(f"Connecting to SMTP server: {SMTP_SERVER}:{SMTP_PORT}")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            if SMTP_PORT != 465: # 465 is typically SSL from the start, 587 uses STARTTLS
                server.starttls()
                server.ehlo()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER.split(','), msg.as_string())
            print(f"Email notification sent successfully to {EMAIL_RECEIVER}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP Authentication Error: {e}. Check credentials or 'less secure app' settings for Gmail.")
    except Exception as e:
        print(f"Error sending email: {e}")

def create_email_html_body(job_id, object_name, status, total_submitted, total_processed, total_successful, total_failed, start_time_str, end_time_str):
    # Calculate success rate and processing time
    success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
    # Basic duration calculation
    duration_str = "N/A"
    if start_time_str and end_time_str:
        try:
            fmt = '%Y-%m-%d %H:%M:%S'
            tstart = datetime.strptime(start_time_str, fmt)
            tend = datetime.strptime(end_time_str, fmt)
            duration = tend - tstart
            duration_str = str(duration).split('.')[0] # Remove microseconds
        except ValueError:
            duration_str = "Error calculating duration"


    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Salesforce Bulk Job Report</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f4f7f6;
                color: #333;
            }}
            .container {{
                width: 90%;
                max-width: 700px;
                margin: 20px auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 8px 25px rgba(0,0,0,0.1);
                overflow: hidden; /* For border-radius to affect children */
            }}
            .header {{
                background: linear-gradient(135deg, #007bff, #0056b3); /* Modern blue gradient */
                color: white;
                padding: 30px 25px;
                text-align: center;
                border-bottom: 5px solid #004085;
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
                font-weight: 600;
                letter-spacing: 0.5px;
            }}
            .content {{
                padding: 25px;
            }}
            .content h2 {{
                color: #0056b3;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 8px;
                margin-top: 0;
                font-size: 22px;
            }}
            .status-badge {{
                display: inline-block;
                padding: 8px 15px;
                border-radius: 20px;
                font-weight: bold;
                font-size: 16px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 15px;
            }}
            .status-JobComplete {{ background-color: #28a745; color: white; }} /* Green */
            .status-Failed {{ background-color: #dc3545; color: white; }} /* Red */
            .status-Aborted {{ background-color: #ffc107; color: black; }} /* Yellow */
            .status-InProgress, .status-UploadComplete {{ background-color: #17a2b8; color: white; }} /* Teal */

            .summary-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 15px;
                margin-top: 20px;
            }}
            .summary-item {{
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
                border: 1px solid #e9ecef;
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }}
            .summary-item:hover {{
                transform: translateY(-5px);
                box-shadow: 0 6px 12px rgba(0,0,0,0.1);
            }}
            .summary-item .label {{
                font-size: 14px;
                color: #6c757d;
                display: block;
                margin-bottom: 5px;
            }}
            .summary-item .value {{
                font-size: 22px;
                font-weight: bold;
                color: #0056b3;
            }}
            .summary-item .value.success {{ color: #28a745; }}
            .summary-item .value.error {{ color: #dc3545; }}
            .footer {{
                text-align: center;
                padding: 20px;
                font-size: 12px;
                color: #777;
                background-color: #e9ecef;
                border-top: 1px solid #dee2e6;
            }}
            .timestamp {{ font-style: italic; }}
            /* Animation for numbers - simple fade in */
            @keyframes fadeIn {{
                from {{ opacity: 0; transform: translateY(10px); }}
                to {{ opacity: 1; transform: translateY(0); }}
            }}
            .value {{
                animation: fadeIn 0.5s ease-out;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Salesforce Bulk Job Report</h1>
            </div>
            <div class="content">
                <h2>Job Summary: {object_name}</h2>
                <p><span class="status-badge status-{status}">{status}</span></p>
                <p><strong>Job ID:</strong> {job_id}</p>
                <p class="timestamp"><strong>Processing Time:</strong> {duration_str} (Started: {start_time_str}, Ended: {end_time_str})</p>

                <div class="summary-grid">
                    <div class="summary-item">
                        <span class="label">Total Submitted</span>
                        <span class="value">{total_submitted}</span>
                    </div>
                    <div class="summary-item">
                        <span class="label">Total Processed</span>
                        <span class="value">{total_processed}</span>
                    </div>
                    <div class="summary-item">
                        <span class="label">Successfully Processed</span>
                        <span class="value success">{total_successful}</span>
                    </div>
                    <div class="summary-item">
                        <span class="label">Failed Records</span>
                        <span class="value error">{total_failed}</span>
                    </div>
                     <div class="summary-item">
                        <span class="label">Success Rate</span>
                        <span class="value {'success' if success_rate > 90 else 'error' if success_rate < 70 else ''}">{success_rate:.2f}%</span>
                    </div>
                </div>
                <p style="margin-top: 25px;">Please find attached CSV files for detailed successful and failed records, if any.</p>
            </div>
            <div class="footer">
                This is an automated report. Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}
            </div>
        </div>
    </body>
    </html>
    """
    return html

def process_bulk_upsert(sf, df_data, object_name, external_id_field):
    if df_data.empty:
        print(f"No data to process for {object_name}.")
        return

    total_submitted_for_job = len(df_data)
    job_start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    csv_buffer = io.StringIO()
    df_data.to_csv(csv_buffer, index=False, lineterminator='\n')
    csv_content = csv_buffer.getvalue()

    job_info = create_bulk_job(object_name, 'upsert', external_id_field)
    if not job_info:
        # Send failure email if job creation fails
        job_end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        html_body = create_email_html_body("N/A", object_name, "JobCreationError", total_submitted_for_job, 0,0,0, job_start_time_str, job_end_time_str)
        send_email_with_attachments(f"FAILED: Salesforce Bulk Job for {object_name}", html_body)
        integration_log = {
            'Status_Code__c': '500',
            'Message__c': f"FAILED: Salesforce Bulk Job for {object_name}",
            'Request_Payload__c': 'None',
            'Response_Payload__c': '',
            'Log_Type__c': 'Python Integration'
        }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return

    job_id = job_info['id']
    if not upload_job_data(job_id, csv_content):
        job_end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        html_body = create_email_html_body(job_id, object_name, "UploadFailed", total_submitted_for_job, 0,0,0, job_start_time_str, job_end_time_str)
        send_email_with_attachments(f"FAILED: Salesforce Bulk Job Upload for {object_name} (Job ID: {job_id})", html_body)
        integration_log = {
            'Status_Code__c': '500',
            'Message__c': f"FAILED: Salesforce Bulk Job Upload for {object_name} (Job ID: {job_id})",
            'Request_Payload__c': 'None',
            'Response_Payload__c': str(job_id),
            'Log_Type__c': 'Python Integration'
        }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return

    closed_job_info = close_job(job_id)
    if not closed_job_info:
        job_end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        html_body = create_email_html_body(job_id, object_name, "JobCloseFailed", total_submitted_for_job, 0,0,0, job_start_time_str, job_end_time_str)
        send_email_with_attachments(f"FAILED: Salesforce Bulk Job Close for {object_name} (Job ID: {job_id})", html_body)
        integration_log = {
            'Status_Code__c': '500',
            'Message__c': f"FAILED: Salesforce Bulk Job Close for {object_name} (Job ID: {job_id})",
            'Request_Payload__c': 'None',
            'Response_Payload__c': str(job_id),
            'Log_Type__c': 'Python Integration'
        }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return

    final_job_status = monitor_job_status(job_id)
    job_end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    attachments_for_email = []
    successful_csv_data = None
    failed_csv_data = None

    if final_job_status:
        state = final_job_status.get('state', 'Unknown')
        processed = final_job_status.get('numberRecordsProcessed', 0)
        failed = final_job_status.get('numberRecordsFailed', 0)
        # For Bulk API 2.0, successful is processed - failed.
        # totalRecords in jobInfo is only for Bulk API 1.0.
        # numberRecordsProcessed for Bulk API 2.0 job info means records Salesforce attempted to process.
        successful = processed - failed 

        result = f"Job {job_id} ({object_name}) final state: {state}, Processed: {processed}, Successful: {successful}, Failed: {failed}"
        print(result)

        if successful > 0:
            successful_csv_data = get_job_results(job_id, "successfulResults")
            if successful_csv_data:
                attachments_for_email.append((f"successful_records_{object_name}_{job_id}.csv", successful_csv_data))
        
        if failed > 0:
            failed_csv_data = get_job_results(job_id, "failedResults")
            if failed_csv_data:
                attachments_for_email.append((f"failed_records_{object_name}_{job_id}.csv", failed_csv_data))
        
        email_subject_status = "SUCCESS" if state == 'JobComplete' and failed == 0 else "PARTIAL_SUCCESS" if state == 'JobComplete' and failed > 0 else "FAILED"
        email_subject = f"{email_subject_status}: Salesforce Bulk Job for {object_name} (ID: {job_id})"
        html_body = create_email_html_body(job_id, object_name, state, total_submitted_for_job, processed, successful, failed, job_start_time_str, job_end_time_str)
        send_email_with_attachments(email_subject, html_body, attachments_for_email)
        integration_log = {
            'Status_Code__c': '200',
            'Message__c': f"Email sent successfully with result",
            'Request_Payload__c': '',
            'Response_Payload__c': str(result),
            'Log_Type__c': 'Python Integration'
        }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return result

    else:
        print(f"Could not determine final status for job {job_id} ({object_name}). Sending failure notification.")
        html_body = create_email_html_body(job_id, object_name, "MonitoringTimeoutOrError", total_submitted_for_job, 0,0,0, job_start_time_str, job_end_time_str)
        send_email_with_attachments(f"ERROR: Salesforce Bulk Job Monitoring for {object_name} (ID: {job_id})", html_body)
        integration_log = {
            'Status_Code__c': '500',
            'Message__c': 'Monitoring timeout or error occurred.',
            'Request_Payload__c': 'None',
            'Response_Payload__c': str(job_id),
            'Log_Type__c': 'Python Integration'
        }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return "Monitoring timeout or error occurred."

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%m-%d-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return date_str  # fallback if neither format matches

def prepare_contact_medical_license_records(sf, df, field_mapping):
    """
    Create or update contact records in Salesforce using bulk upsert.
    Create or update medical license records in Salesforce using bulk upsert.

    Parameters:
        sf (Salesforce): Authenticated Salesforce connection object.
        df (pd.DataFrame): Source data.
        field_mapping (dict): Mapping of source fields to Salesforce Contact fields.
        # external_id_field (str): API name of the external ID field.

    Returns:
        dict: Summary of successes and failures.
    """
    try:
        state_code_map, country_code_map = get_state_code_mapping(sf)
        con_external_id_field = "ABO_Id__c"  # External ID field for upsert
        med_external_id_field = "Composite_Key__c"  # External ID field for upsert
        med_external_reference_field = "Contact__r.ABO_Id__c"  # External reference field for upsert
        contact_records_to_create = []
        medical_records_to_create = []

        for idx, row in df.iterrows():
            board_id = row.get("BoardUniqueID") or row.get("Board_Id")
            composite_key = '-'.join(
                parse_date(str(row.get('LicenseExpireDate'))) 
                if k == 'LicenseExpireDate' and row.get('LicenseExpireDate') 
                else str(row.get(k)).strip()
                for k in ['BoardUniqueID', 'LicenseNumber', 'LicenseExpireDate']
                if row.get(k) is not None and str(row.get(k)).strip().lower() != 'nan' and str(row.get(k)).strip() != 'NAN' and str(row.get(k)).strip() != ''
            )

            contact = {}
            ml_record = {}

            # Map fields
            for source_field, target_field in field_mapping.get('Contact', {}).items():
                value = str(row.get(source_field)).strip()

                if value is not None and ("#" in str(value) or "nan" in str(value) or "NAN" in str(value)):
                    continue

                if "gender" in source_field.lower() and pd.notna(value):
                    if value == "M":
                        value = "Male"
                    elif value == "F":
                        value = "Female"
                    else:
                        value = ""

                if "date" in source_field.lower() and pd.notna(value):
                    try:
                        value = datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
                        logger.info(f"Parsed date {value} for field {source_field}")
                    except ValueError:
                        logger.warning(f"Invalid date in row {idx}: {value}")
                        continue

                if pd.notna(value):
                    contact[target_field] = value

            # Map fields
            for source_field, target_field in field_mapping.get('Medical_License__c', {}).items():
                value = str(row.get(source_field)).strip()

                if value is not None and ("#" in str(value) or "nan" in str(value) or "NAN" in str(value)):
                    continue
                
                if "npi" in source_field.lower() and pd.notna(value):
                    # Remove leading zeros from NPI
                    value = int(float(value))
                    logger.info(f"Processed NPI {value} for field {source_field}")

                if "state" in source_field.lower() and pd.notna(value):
                    # Map state codes to state names
                    if value in state_code_map:
                        ml_record["Country__c"] = country_code_map.get(value)
                        value = state_code_map[value]
                    else:
                        value = "Other"
                        ml_record["Country__c"] = "Other"

                if "date" in source_field.lower() and pd.notna(value):
                    try:
                        value = datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
                        logger.info(f"Parsed date {value} for field {source_field}")
                    except ValueError:
                        logger.warning(f"Invalid date in row {idx}: {value}")
                        continue

                if pd.notna(value):
                    ml_record[target_field] = value

            if contact:
                if contact not in contact_records_to_create:
                    contact_records_to_create.append(contact)
            
            if ml_record:
                if ml_record not in medical_records_to_create:
                    ml_record[med_external_id_field] = composite_key
                    logger.info(f"Composite Key: {composite_key}")
                    ml_record[med_external_reference_field] = board_id
                    medical_records_to_create.append(ml_record)
        
        # Convert to DataFrames
        contact_df = pd.DataFrame(contact_records_to_create)
        medical_df = pd.DataFrame(medical_records_to_create)
        contact_headers = list(contact_df.columns)
        medical_headers = list(medical_df.columns)

        df_contacts = contact_df.drop_duplicates(subset= contact_headers)
        df_medical = medical_df.drop_duplicates(subset= medical_headers)
        logger.info(f"Contact DataFrame after removing duplicates: {len(df_contacts)} and before removing duplicates: {len(contact_df)}")
        logger.info(f"Medical DataFrame after removing duplicates: {len(df_medical)} and before removing duplicates: {len(medical_df)}") 

        # Optionally inspect
        logger.info("Contact DataFrame:")
        logger.info(contact_df)
        logger.info("Medical License DataFrame:")
        logger.info(medical_df)
        result = None
        if not df_contacts.empty:
            df_contacts = df_contacts.fillna('')
            result = process_bulk_upsert(sf, df_contacts, 'Contact', con_external_id_field)
        else:
            logger.info("No contact records to process.")
            send_email_with_attachments(
                f"INFO: Salesforce Bulk Script - No Contacts Processed for Contact",
                f"<html><body><h1>No Data</h1><p>No valid Contact records were found in <strong>File Shared</strong> for processing.</p></body></html>"
            )
            integration_log = {
                'Status_Code__c': '200',
                'Message__c': "No contact records to process.",
                'Request_Payload__c': '',
                'Response_Payload__c': '',
                'Log_Type__c': 'Python Integration'
            }
            try:
                sf.Integration_Log__c.create(integration_log)
            except Exception as log_error:
                logger.error(f"Failed to create integration log: {str(log_error)}")
            result = "No contact records to process."
        if not df_medical.empty:
            df_medical = df_medical.fillna('')
            result = process_bulk_upsert(sf, df_medical, 'Medical_License__c', med_external_id_field)
        else:
            logger.info("No medical license records to process.")
            send_email_with_attachments(
                f"INFO: Salesforce Bulk Script - No Medical Licenses Processed for Medical License",
                f"<html><body><h1>No Data</h1><p>No valid Medical License records were found in <strong>File Shared</strong> for processing after filtering.</p></body></html>"
            )
            integration_log = {
                'Status_Code__c': '200',
                'Message__c': "No Medical license records to process.",
                'Request_Payload__c': '',
                'Response_Payload__c': '',
                'Log_Type__c': 'Python Integration'
            }
            try:
                sf.Integration_Log__c.create(integration_log)
            except Exception as log_error:
                logger.error(f"Failed to create integration log: {str(log_error)}")
            result += "No Medical license records to process."
        return result


    except Exception as e:
        logger.error(f"Error creating contact: {str(e)}", exc_info=True)
        raise SalesforceAPIError(f"Error creating contact: {str(e)}", status_code=500)

def prepare_disiciplinary_records(sf, df, file_type, field_mapping):
    """
    Create or update disciplinary records in Salesforce using bulk upsert.

    Parameters:
        sf (Salesforce): Authenticated Salesforce connection object.
        df (pd.DataFrame): Source data.
        field_mapping (dict): Mapping of source fields to Salesforce Disciplinary fields.

    Returns:
        dict: Summary of successes and failures.
    """
    try:
        sf_object_name = "Disciplinary_Actions__c"
        external_id_field = "DANS_Composite_ID__c"  # External ID field for upsert
        disciplinary_records_to_create = []
        record_type_suffix = file_type.split('_')[-1].capitalize()
        record_type_id = None

        result = sf.query(f"""
            SELECT Id, Name 
            FROM RecordType 
            WHERE SObjectType = '{sf_object_name}' 
            AND Name LIKE '%{record_type_suffix}%'
        """)
        logger.info(f"Querying RecordType for {sf_object_name} with suffix '{record_type_suffix}'")
        logger.info(f"RecordType query result: {result}")
        # Extract RecordTypeId
        if result['totalSize'] > 0:
            record_type_id = result['records'][0]['Id']
            print(f"Matched RecordType: {result['records'][0]['Name']}")
        else:
            print("RecordType not found.")

        for idx, row in df.iterrows():
            composite_key = '-'.join(
                parse_date(str(row.get('Order_Date'))) 
                if k == 'Order_Date' and row.get('Order_Date') 
                else str(row.get(k))
                for k in ['Board_Id', 'Statistical_Code', 'action_code', 'Order_Date','basis_code']
                if row.get(k) is not None
            )

            dis_record = {}

            # Map fields
            for source_field, target_field in field_mapping.get('Disciplinary_Actions__c', {}).items():
                value = str(row.get(source_field)).strip()

                if value is not None and "#" in str(value):
                    continue

                if "date" in source_field.lower() and pd.notna(value):
                    try:
                        value = datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
                        logger.info(f"Parsed date {value} for field {source_field}")
                    except ValueError:
                        logger.warning(f"Invalid date in row {idx}: {value}")
                        continue

                if pd.notna(value):
                    dis_record[target_field] = value

            if dis_record:
                dis_record[external_id_field] = composite_key
                if record_type_id:
                    dis_record['RecordTypeId'] = record_type_id
                logger.info(f"Composite Key: {composite_key}")
                disciplinary_records_to_create.append(dis_record)

        # Convert to DataFrame
        dis_df = pd.DataFrame(disciplinary_records_to_create)
        dis_headers = list(dis_df.columns)
        df_disciplinary = dis_df.drop_duplicates(subset= dis_headers)
        logger.info(f"Disciplinary DataFrame after removing duplicates: {len(df_disciplinary)} and before removing duplicates: {len(dis_df)}")
        # Optionally inspect
        logger.info("Disciplinary DataFrame:")
        logger.info(dis_df)
        result = None
        if not df_disciplinary.empty:
            df_disciplinary = df_disciplinary.fillna('')
            result = process_bulk_upsert(sf, df_disciplinary, sf_object_name, external_id_field)
        else:
            logger.info("No disciplinary records to process.")
            send_email_with_attachments(
                f"INFO: Salesforce Bulk Script - No Disciplinary Records Processed for Disciplinary Action",
                f"<html><body><h1>No Data</h1><p>No valid Disciplinary Action records were found in <strong>File Shared</strong> for processing after filtering.</p></body></html>"
            )
            integration_log = {
                'Status_Code__c': '500',
                'Message__c': "No disciplinary records to process.",
                'Request_Payload__c': 'None',
                'Response_Payload__c': '',
                'Log_Type__c': 'Python Integration'
            }
            try:
                sf.Integration_Log__c.create(integration_log)
            except Exception as log_error:
                logger.error(f"Failed to create integration log: {str(log_error)}")
            result = "No disciplinary records to process."
        return result

    except Exception as e:
        logger.error(f"Error creating contact: {str(e)}", exc_info=True)
        raise SalesforceAPIError(f"Error creating contact: {str(e)}", status_code=500) 

def main(file_name,file_type,file_extension):
    """Main function to run the script."""
    try:
        sf = get_salesforce_connection()
        query = f"SELECT Id, Title, VersionDataUrl FROM ContentVersion WHERE Title = '{file_name}' ORDER BY CreatedDate DESC LIMIT 1"
        df, *dicts = get_salesforce_file(sf, query, file_type, file_extension == ".csv")
        # Initialize mappings
        LIDS_mapping = {}
        DANS_candidateMapping = {}
        DANS_diplomateMapping = {}

        if len(dicts) == 1:
            LIDS_mapping = dicts[0]
        else:
            DANS_candidateMapping, DANS_diplomateMapping = dicts

        # logger.info the DataFrame & Mappings
        logger.info("DataFrame Head:"+df.head().to_string())
        logger.info("LIDS Mapping:" + str(LIDS_mapping))
        logger.info("DANS Candidates Mapping:" + str(DANS_candidateMapping))
        logger.info("DANS Diplomates Mapping:" + str(DANS_diplomateMapping))

        if(file_type == "LIDS"):
            result = prepare_contact_medical_license_records(sf, df, LIDS_mapping)
        elif(file_type == "DANS_Candidate"):
            result = prepare_disiciplinary_records(sf, df, file_type, DANS_candidateMapping)
        elif(file_type == "DANS_Diplomate"):
            result = prepare_disiciplinary_records(sf, df, file_type, DANS_diplomateMapping)

        return result
        
    except SalesforceAPIError as e:
        logger.error(f"Salesforce API error: {e.message}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)

@app.get("/create")
def create_item(file_name: str, file_type: str, file_extension: str):
    result = main(file_name,file_type,file_extension)
    return result

@app.get('/connection')
def test_connection():
    """Test the Salesforce connection."""
    try:
        # Test Salesforce connection
        sf = get_salesforce_connection()
        
        # Check if we can query a simple object
        result = sf.query("SELECT Id FROM Account LIMIT 1")
        
        # Return success with some basic info
        return {
            "status": "success",
            "connection": "established",
            "query_result": f"Found {result.get('totalSize', 0)} records"
        }
    except Exception as e:
        logger.error(f"Connection test error: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }, 500

@app.get("/")
def health_check():
    """Simple health check endpoint."""
    return "Inbound API is running"

if __name__ == "__main__":
    # test_connection()
    main()
    logger.info("This module is not meant to be run directly. Please use it as part of a larger application.")