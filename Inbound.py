from simple_salesforce import Salesforce
from functools import lru_cache
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict
from io import StringIO
import pandas as pd
import requests
import logging
import json
import html
import os

# for email sending
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime

# for web services
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    """Simple health check endpoint."""
    return "Inbound API is running"

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
    logger.info("Establishing Salesforce connection")
    try:
        sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_SECURITY_TOKEN"),
            domain=os.getenv("SF_DOMAIN"),
        )
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

def get_salesforce_file(sf,query,is_csv):
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
                    df = pd.read_csv(csv_content, delimiter="\t", on_bad_lines='warn')
                    print(df.head())
                
                # Display the table with headers
                print("CSV file successfully downloaded and converted to table:")
                # print(df)

                query = "SELECT Id,DANS_Candidates_Field_Mapping__c, DANS_Diplomates_Field_Mapping__c,  LIDS_All_Active_Field_Mapping__c FROM ABOP_Migration__c WHERE Is_Active__c =true AND XML_Type__c = 'LIDS'"
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

                        # Print the mappings
                        # print(f"DANS Candidates Mapping: {dans_candidates_mapping}")
                        # print(f"DANS Diplomates Mapping: {dans_diplomates_mapping}")
                        # print(f"LIDS All Active Mapping: {lids_all_active_mapping}")

                        if lids_all_active_mapping:
                            return df, lids_all_active_mapping
                        else:
                            return df, dans_candidates_mapping, dans_diplomates_mapping
                
            except requests.exceptions.RequestException as e:
                print(f"Error downloading the file: {e}")
        else:
            print("No ContentVersion record found with the specified title.")
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
    print(f"Response: {response.json()}")

    if response.status_code != 200:
        logger.error(f"Failed to post batch: {response.text}", exc_info=True)
        # raise SalesforceAPIError(f"Failed to post batch: {response.text}", status_code=response.status_code)

    return response.json().get("results", [])

def fetch_all_contact_ids(sf: Salesforce):
    """
    Fetches all Contact records from Salesforce and returns a dictionary
    mapping ABO_Id__c to Contact Id.

    Args:
        sf (Salesforce): An authenticated Salesforce session.

    Returns:
        dict: A dictionary where keys are ABO_Id__c values and values are Contact Ids.
    """
    try:
        query = "SELECT Id, ABO_Id__c FROM Contact WHERE ABO_Id__c != null"
        records = sf.query_all(query)["records"]

        contact_map = {
            record["ABO_Id__c"]: record["Id"]
            for record in records
            if "ABO_Id__c" in record and "Id" in record
        }

        logger.info(f"Fetched {len(contact_map)} Contact ABO ID mappings from Salesforce.")
        return contact_map

    except Exception as e:
        logger.error(f"Error fetching Contact mappings from Salesforce: {e}")
        return {}

def create_contact_records(sf, df, field_mapping):
    """
    Create or update contact records in Salesforce using bulk upsert.

    Parameters:
        sf (Salesforce): Authenticated Salesforce connection object.
        df (pd.DataFrame): Source data.
        field_mapping (dict): Mapping of source fields to Salesforce Contact fields.
        external_id_field (str): API name of the external ID field.

    Returns:
        dict: Summary of successes and failures.
    """
    try:
        external_id_field = "ABO_Id__c"  # External ID field for upsert
        abo_ids = set()
        contact_records_to_create = []

        for idx, row in df.iterrows():
            board_id = row.get("BoardUniqueID")
            if not board_id or board_id in abo_ids:
                logger.info(f"Skipping existing or missing BoardUniqueID for ABO_Id__c: {row.get('ABO_Id__c')}")
                continue

            contact = {}
            abo_ids.add(board_id)

            # Map fields
            for source_field, target_field in field_mapping.get('Contact', {}).items():
                value = row.get(source_field)

                if value is not None and "#" in str(value):
                    continue

                if "gender" in source_field.lower() and pd.notna(value):
                    if value == "M":
                        value = "Male"
                    elif value == "F":
                        value = "Female"

                if "date" in source_field.lower() and pd.notna(value):
                    try:
                        value = datetime.datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        logger.warning(f"Invalid date in row {idx}: {value}")
                        continue

                if pd.notna(value):
                    contact[target_field] = value

            if contact:
                contact_records_to_create.append(contact)

        # Perform Bulk Upsert
        results = sf.bulk.Contact.upsert(contact_records_to_create, external_id_field=external_id_field)

        # Collect success/failure details
        successes = [res for res in results if res.get('success')]
        failures = [res for res in results if not res.get('success')]

        logger.info(f"Bulk upsert complete. Successes: {len(successes)}, Failures: {len(failures)}")

        return {
            "success_count": len(successes),
            "failure_count": len(failures),
            "failures": failures
        }

    except Exception as e:
        logger.error(f"Error creating contact: {str(e)}", exc_info=True)
        raise SalesforceAPIError(f"Error creating contact: {str(e)}", status_code=500)

def create_medical_license_records(sf, df, field_mapping):
    """
    Create or update medical license records in Salesforce using bulk upsert.

    Parameters:
        sf (Salesforce): Authenticated Salesforce connection object.
        df (pd.DataFrame): Source data.
        field_mapping (dict): Mapping of source fields to Salesforce Contact fields.
        external_id_field (str): API name of the external ID field.

    Returns:
        dict: Summary of successes and failures.
    """
    try:
        state_code_map, country_code_map = get_state_code_mapping(sf)
        contact_with_aboIds = fetch_all_contact_ids(sf)
        medical_records_to_create = []

        for idx, row in df.iterrows():

            ml_record = {}

            # Map fields
            for source_field, target_field in field_mapping.get('Medical_License__c', {}).items():
                value = row.get(source_field)

                if value is not None and "#" in str(value):
                    continue

                if "state" in source_field.lower() and pd.notna(value):
                    # Map state codes to state names
                    if value in state_code_map:
                        ml_record["Country__c"] = country_code_map.get(value)
                        value = state_code_map[value]

                if "date" in source_field.lower() and pd.notna(value):
                    try:
                        value = datetime.datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        logger.warning(f"Invalid date in row {idx}: {value}")
                        continue

                if pd.notna(value):
                    ml_record[target_field] = value

            if ml_record:
                ml_record["Contact__c"] = contact_with_aboIds.get(str(row.get("BoardUniqueID")))
                medical_records_to_create.append(ml_record)

        # Perform Bulk Insert
        results = sf.bulk.Medical_License__c.insert(medical_records_to_create)

        # Collect success/failure details
        successes = [res for res in results if res.get('success')]
        failures = [res for res in results if not res.get('success')]

        logger.info(f"Bulk upsert complete. Successes: {len(successes)}, Failures: {len(failures)}")

        return {
            "success_count": len(successes),
            "failure_count": len(failures),
            "failures": failures
        }

    except Exception as e:
        logger.error(f"Error creating contact: {str(e)}", exc_info=True)
        raise SalesforceAPIError(f"Error creating contact: {str(e)}", status_code=500)

def send_upsert_summary_email(user_email: str, results: Dict, smtp_config: dict):
    """
    Sends a beautifully styled HTML email to the user with a summary of Salesforce upload results.

    Parameters:
        user_email (str): Recipient email address (logged-in user).
        results (Dict): Dictionary containing results for contacts and medical licenses.
            Format: {
                "contacts": {"success_count": int, "failure_count": int, "failures": List},
                "medical_licenses": {"success_count": int, "failure_count": int, "failures": List}
            }
        smtp_config (dict): SMTP config: host, port, username, password, sender_email.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Extract results
    contacts_results = results.get("contacts", {})
    licenses_results = results.get("medical_licenses", {})
    
    # Calculate totals
    total_success = contacts_results.get("success_count", 0) + licenses_results.get("success_count", 0)
    total_failures = contacts_results.get("failure_count", 0) + licenses_results.get("failure_count", 0)
    total_records = total_success + total_failures
    success_rate = (total_success / total_records * 100) if total_records > 0 else 0
    
    # HTML message structure with enhanced CSS
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Salesforce Upload Summary</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
            
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }}
            
            body {{
                font-family: 'Roboto', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                background-color: #f9f9f9;
                padding: 20px;
            }}
            
            .email-container {{
                max-width: 650px;
                margin: 0 auto;
                background: white;
                border-radius: 10px;
                overflow: hidden;
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            }}
            
            .email-header {{
                background: linear-gradient(135deg, #0047ab, #0073e6);
                color: white;
                padding: 25px;
                text-align: center;
            }}
            
            .email-header h1 {{
                margin: 0;
                font-size: 24px;
                font-weight: 700;
            }}
            
            .email-body {{
                padding: 25px;
            }}
            
            .summary-box {{
                background-color: #f7fafc;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 25px;
                border-left: 4px solid #0047ab;
            }}
            
            .timestamp {{
                color: #666;
                font-size: 14px;
                margin-bottom: 10px;
            }}
            
            .stats-container {{
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 15px;
                margin: 20px 0;
            }}
            
            .stat-card {{
                flex: 1;
                min-width: 120px;
                background: white;
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
                text-align: center;
            }}
            
            .stat-card.success {{
                border-top: 3px solid #10b981;
            }}
            
            .stat-card.failure {{
                border-top: 3px solid #ef4444;
            }}
            
            .stat-card.total {{
                border-top: 3px solid #3b82f6;
            }}
            
            .stat-card.rate {{
                border-top: 3px solid #8b5cf6;
            }}
            
            .stat-value {{
                font-size: 28px;
                font-weight: 700;
                margin: 5px 0;
            }}
            
            .stat-label {{
                color: #6b7280;
                font-size: 14px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            
            .success-value {{
                color: #10b981;
            }}
            
            .failure-value {{
                color: #ef4444;
            }}
            
            .total-value {{
                color: #3b82f6;
            }}
            
            .rate-value {{
                color: #8b5cf6;
            }}
            
            .section-title {{
                font-size: 18px;
                font-weight: 600;
                margin: 25px 0 15px;
                color: #0047ab;
                border-bottom: 1px solid #e5e7eb;
                padding-bottom: 8px;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 15px 0;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
            }}
            
            th {{
                background-color: #f3f4f6;
                padding: 12px 15px;
                text-align: left;
                font-weight: 600;
                color: #374151;
                font-size: 14px;
            }}
            
            td {{
                padding: 12px 15px;
                border-top: 1px solid #e5e7eb;
                font-size: 14px;
            }}
            
            tr:nth-child(even) {{
                background-color: #f9fafb;
            }}
            
            .success-icon {{
                color: #10b981;
                font-weight: bold;
            }}
            
            .fail-icon {{
                color: #ef4444;
                font-weight: bold;
            }}
            
            .message-cell {{
                max-width: 300px;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}
            
            .error-list {{
                margin-top: 15px;
            }}
            
            .error-item {{
                background-color: #fef2f2;
                border-left: 3px solid #ef4444;
                padding: 10px 15px;
                margin-bottom: 10px;
                border-radius: 4px;
                font-size: 14px;
            }}
            
            .email-footer {{
                background-color: #f3f4f6;
                padding: 20px;
                text-align: center;
                color: #6b7280;
                font-size: 14px;
            }}
            
            .footer-logo {{
                font-weight: 700;
                margin-top: 10px;
                color: #0047ab;
            }}

            .progress-bar-container {{
                width: 100%;
                height: 10px;
                background-color: #e5e7eb;
                border-radius: 5px;
                margin: 15px 0;
                overflow: hidden;
            }}
            
            .progress-bar {{
                height: 100%;
                background: linear-gradient(90deg, #10b981, #3b82f6);
                width: {success_rate}%;
                border-radius: 5px;
            }}
            
            @media (max-width: 600px) {{
                .stats-container {{
                    flex-direction: column;
                }}
                
                .stat-card {{
                    width: 100%;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="email-header">
                <h1>Salesforce Integration Summary</h1>
            </div>
            
            <div class="email-body">
                <div class="summary-box">
                    <div class="timestamp">📅 <strong>Date:</strong> {timestamp}</div>
                    <p>Your Salesforce data integration has completed processing.</p>
                    
                    <div class="progress-bar-container">
                        <div class="progress-bar"></div>
                    </div>
                    
                    <div class="stats-container">
                        <div class="stat-card success">
                            <div class="stat-label">Successful</div>
                            <div class="stat-value success-value">{total_success}</div>
                        </div>
                        
                        <div class="stat-card failure">
                            <div class="stat-label">Failed</div>
                            <div class="stat-value failure-value">{total_failures}</div>
                        </div>
                        
                        <div class="stat-card total">
                            <div class="stat-label">Total</div>
                            <div class="stat-value total-value">{total_records}</div>
                        </div>
                        
                        <div class="stat-card rate">
                            <div class="stat-label">Success Rate</div>
                            <div class="stat-value rate-value">{success_rate:.1f}%</div>
                        </div>
                    </div>
                </div>
                
                <h2 class="section-title">Contacts</h2>
                <table>
                    <tr>
                        <th>Category</th>
                        <th>Successful</th>
                        <th>Failed</th>
                        <th>Total</th>
                    </tr>
                    <tr>
                        <td>Contacts</td>
                        <td class="success-icon">{contacts_results.get("success_count", 0)} ✅</td>
                        <td class="fail-icon">{contacts_results.get("failure_count", 0)} {' ❌' if contacts_results.get("failure_count", 0) > 0 else ''}</td>
                        <td>{contacts_results.get("success_count", 0) + contacts_results.get("failure_count", 0)}</td>
                    </tr>
                </table>
    """

    # Add contact failures if any
    if contacts_results.get("failure_count", 0) > 0 and contacts_results.get("failures"):
        html += """
                <div class="error-list">
                    <h3>Contact Failures:</h3>
        """
        for failure in contacts_results.get("failures", []):
            html += f"""
                    <div class="error-item">{failure}</div>
            """
        html += """
                </div>
        """

    # Medical Licenses section
    html += f"""
                <h2 class="section-title">Medical Licenses</h2>
                <table>
                    <tr>
                        <th>Category</th>
                        <th>Successful</th>
                        <th>Failed</th>
                        <th>Total</th>
                    </tr>
                    <tr>
                        <td>Medical Licenses</td>
                        <td class="success-icon">{licenses_results.get("success_count", 0)} ✅</td>
                        <td class="fail-icon">{licenses_results.get("failure_count", 0)} {' ❌' if licenses_results.get("failure_count", 0) > 0 else ''}</td>
                        <td>{licenses_results.get("success_count", 0) + licenses_results.get("failure_count", 0)}</td>
                    </tr>
                </table>
    """

    # Add license failures if any
    if licenses_results.get("failure_count", 0) > 0 and licenses_results.get("failures"):
        html += """
                <div class="error-list">
                    <h3>Medical License Failures:</h3>
        """
        for failure in licenses_results.get("failures", []):
            html += f"""
                    <div class="error-item">{failure}</div>
            """
        html += """
                </div>
        """

    # Close email
    html += """
            </div>
            
            <div class="email-footer">
                <p>This is an automated message. Please do not reply to this email.</p>
                <p>If you have any questions, please contact your system administrator.</p>
                <p class="footer-logo">Salesforce Integration Bot 🤖</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Create email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Salesforce Integration Summary Report"
    msg["From"] = smtp_config["sender_email"]
    msg["To"] = user_email

    msg.attach(MIMEText(html, "html"))

    # Send via SMTP
    try:
        with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
            server.starttls()
            server.login(smtp_config["username"], smtp_config["password"])
            server.sendmail(msg["From"], [msg["To"]], msg.as_string())
            print(f"✅ Summary email sent to {user_email}")
            return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False

def main(file_name,file_type,file_extension):
    """Main function to run the script."""
    try:
        sf = get_salesforce_connection()
        query = f"SELECT Id, Title, VersionDataUrl FROM ContentVersion WHERE Title = '{file_name}' ORDER BY CreatedDate DESC LIMIT 1"
        df, *dicts = get_salesforce_file(sf,query,file_extension == ".csv")
        # Initialize mappings
        LIDS_mapping = {}
        DANS_candidateMapping = {}
        DANS_diplomateMapping = {}

        # Responses
        contact_response = {}
        medical_response = {}

        if len(dicts) == 1:
            LIDS_mapping = dicts[0]
        else:
            DANS_candidateMapping, DANS_diplomateMapping = dicts

        # Print the DataFrame & Mappings
        logger.info("DataFrame Head:"+df.head().to_string())
        logger.info("LIDS Mapping:" + str(LIDS_mapping))
        logger.info("DANS Candidates Mapping:" + str(DANS_candidateMapping))
        logger.info("DANS Diplomates Mapping:" + str(DANS_diplomateMapping))

        if(file_type == "LIDS"):
            contact_response = create_contact_records(sf, df, LIDS_mapping)
            medical_response = create_medical_license_records(sf, df, LIDS_mapping)
        elif(file_type == "DANS"):
            contact_response = create_contact_records(sf, df, DANS_candidateMapping)
            #medical_response = create_medical_license_records(sf, df, DANS_diplomateMapping)
        # Print the payload
        logger.info("Responses:")
        logger.info(json.dumps(contact_response, indent=4))
        logger.info(json.dumps(medical_response, indent=4))
        # print(response)

        smtp_config = {
            "host": "smtp.gmail.com",
            "port": 587,
            "username": "aethereus12@gmail.com",
            "password": os.getenv("GMAIL_APP_PASSWORD"),
            "sender_email": "aethereus12@gmail.com"
        }

        # Format them into the expected dictionary structure
        results = {
            "contacts": contact_response,
            "medical_licenses": medical_response
        }
        
        success = send_upsert_summary_email("aethereus12@gmail.com",results, smtp_config)
        integration_log = {}
        if success:
            logger.info("Email sent successfully.")
            integration_log = {
                'Status_Code__c': '200',
                'Message__c': f"Email sent successfully to {smtp_config['username']}",
                'Request_Payload__c': '',
                'Response_Payload__c': results,
                'Log_Type__c': 'Python Integration'
            }
        else:
            logger.error("Failed to send email.")
            integration_log = {
                'Status_Code__c': '500',
                'Message__c': 'Email sending failed',
                'Request_Payload__c': 'None',
                'Response_Payload__c': results,
                'Log_Type__c': 'Python Integration'
            }
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
        return results
        
    except SalesforceAPIError as e:
        logger.error(f"Salesforce API error: {e.message}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)

@app.get("/create")
def create_item(file_name: str, file_type: str, file_extension: str):
    result = main(file_name,file_type,file_extension)
    return result

def test_connection():
    """Test the Salesforce connection."""
    try:
        # Test Salesforce connection
        sf = get_salesforce_connection()
        
        # Check if we can query a simple object
        result = sf.query("SELECT Id FROM Account LIMIT 1")
        
        # Return success with some basic info
        return ({
            "status": "success",
            "connection": "established",
            "query_result": f"Found {result.get('totalSize', 0)} records"
        })
    except Exception as e:
        logger.error(f"Connection test error: {str(e)}", exc_info=True)
        return ({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

if __name__ == "__main__":
    test_connection()
    main()
    print("This module is not meant to be run directly. Please use it as part of a larger application.")