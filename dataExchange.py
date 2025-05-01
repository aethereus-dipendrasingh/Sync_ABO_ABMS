from simple_salesforce import Salesforce
from xml.dom.minidom import parseString
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import html
import os
import json
import logging
import base64
from functools import lru_cache
import datetime
import time
from flask import Flask, request, jsonify
from flask_cors import CORS

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

# Initialize Flask application
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

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


def get_state_code_mapping(sf):
    """
    Query State_Code_Mapping__mdt and return a dict with State as key and StateCode as value.
    
    Args:
        sf: Salesforce connection object
        
    Returns:
        dict: Mapping of state names to state codes
    """
    logger.info("Querying State_Code_Mapping__mdt records")
    
    try:
        # Query all records from the custom metadata type
        query = """
            SELECT Label, State__c 
            FROM State_Code_Mapping__mdt
            WHERE Country__c != NULL 
            AND State__c != NULL
        """
        
        result = sf.query(query)
        
        # Create a dictionary with State as key and StateCode as value
        state_to_code_map = {}
        
        if result and 'records' in result:
            for record in result['records']:
                state_code = record.get('Label')
                state = record.get('State__c')
                
                if state_code and state:
                    state_to_code_map[state] = state_code
        
        logger.info(f"Retrieved {len(state_to_code_map)} state-code mappings")
        return state_to_code_map
        
    except Exception as e:
        logger.error(f"Error querying State_Code_Mapping__mdt: {str(e)}", exc_info=True)
        return {}


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


def generate_xml_members(contact_records, metadata_records, main_template, license_template, field_mapping, state_code_map):
    """
    Generate XML members from Salesforce query results.
    
    Args:
        contact_records (dict): Salesforce query results for contacts
        metadata_records (dict): Salesforce query results for metadata
        main_template (str): Main XML template
        license_template (str): License XML template
        field_mapping (str): Field mapping JSON string or dict
        state_code_map (dict): Mapping of state names to state codes
        
    Returns:
        list: List of generated XML content for each contact
    """
    logger.info("Generating XML members")
    start_time = time.time()
    xml_members = []
    
    # Parse field mapping if it's a string
    if isinstance(field_mapping, str):
        try:
            field_mapping = json.loads(field_mapping)
        except Exception as e:
            logger.error(f"Failed to parse field_mapping as JSON: {e}")
            field_mapping = {}

    # Validate contact records
    if not contact_records or 'records' not in contact_records or not contact_records['records']:
        logger.warning("No contact records found in query result")
        return xml_members
    
    contacts = contact_records.get('records', [])
    logger.info(f"Processing {len(contacts)} contact records")
    
    # Validate metadata records
    if not metadata_records or 'records' not in metadata_records or not metadata_records['records']:
        logger.warning("No metadata records found")
        metadata_record = {}
    else:
        metadata_record = metadata_records['records'][0]
        logger.info(f"Using metadata record: {metadata_record.get('Id')}")
    
    # Process each contact
    for idx, contact in enumerate(contacts):
        if not contact:
            logger.warning(f"Skipping empty contact at index {idx}")
            continue
            
        contact_id = contact.get('Id', f"Unknown-{idx}")
        logger.debug(f"Processing contact {idx+1}/{len(contacts)}: {contact_id}")
        
        # Generate license blocks for this contact
        license_blocks = []
        
        # Process medical licenses if they exist
        medical_licenses = contact.get('Medical_Licenses__r')
        if medical_licenses and 'records' in medical_licenses:
            licenses = medical_licenses.get('records', [])
            
            if licenses:
                logger.debug(f"Processing {len(licenses)} licenses for contact {contact_id}")
                for license_idx, license_record in enumerate(licenses):
                    if not license_record:
                        logger.warning(f"Skipping empty license at index {license_idx} for contact {contact_id}")
                        continue
                        
                    # Create a copy of the license XML template for this license
                    license_block = license_template
                    
                    # Replace placeholders in license XML template
                    for placeholder, field_name in field_mapping.items():
                        if placeholder.startswith("{{Medical_License__c."):
                            field = placeholder.split(".")[-1].rstrip("}}")
                            state_value = license_record.get(field_name)

                            # Convert state to string if it's not None
                            value_str = str(state_value) if state_value is not None else ' '

                            if "State" in field:
                                # Try to get the state code from the mapping
                                state_str = str(state_value) if state_value is not None else ''
                                if state_str in state_code_map:
                                    value_str = str(state_code_map[state_str])
                                    logger.info(f"Replaced state '{state_value}' with state code '{value_str}'")
                            
                            license_block = license_block.replace(placeholder, value_str)
                    
                    license_blocks.append(license_block)
        
        # Join all license blocks into a single string
        licenses_block = "\n".join(license_blocks)
        
        # Create a copy of the main XML template for this contact
        contact_xml = main_template
        
        # Replace the licenses block placeholder
        contact_xml = contact_xml.replace("{{LicensesBlock}}", licenses_block)
        
        # Replace all other placeholders using the field mapping
        for placeholder, field_name in field_mapping.items():
            if placeholder.startswith("{{Contact."):
                value = contact.get(field_name)
                
                # Handle different value types
                if isinstance(value, dict) and 'city' in value:
                    value_str = ', '.join(filter(None, [
                        value.get(k) for k in ['street', 'city', 'state', 'postalCode', 'country']
                    ]))
                elif "LastModifiedDate" in placeholder and value:
                    try:
                        value_str = str(datetime.datetime.strptime(
                            value, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d"))
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid date format: {value}")
                        value_str = ' '
                elif value is not None:
                    value_str = str(value)
                else:
                    value_str = ' '
                
                contact_xml = contact_xml.replace(placeholder, value_str)
            elif placeholder.startswith("{{ABO_Setting__mdt."):
                value = metadata_record.get(field_name)
                value_str = str(value) if value is not None else ' '
                contact_xml = contact_xml.replace(placeholder, value_str)
            else:
                contact_xml = contact_xml.replace(placeholder, ' ')
        
        xml_members.append(contact_xml)
    
    end_time = time.time()
    logger.info(f"XML member generation completed in {end_time - start_time:.2f} seconds")
    return xml_members


def upload_xml_to_library(sf, xml_content, title, library_id):
    """
    Upload XML content to Salesforce Content Library.
    
    Args:
        sf: Salesforce connection object
        xml_content: XML content to upload
        title: Title for the content version
        library_id: ID of the content library
        
    Returns:
        str: Download URL for the uploaded file
    """
    logger.info(f"Uploading XML file '{title}' to library {library_id}")
    
    try:
        # Convert to bytes if string
        if isinstance(xml_content, str):
            xml_bytes = xml_content.encode('utf-8')
        else:
            xml_bytes = xml_content
        
        # Encode the XML content to base64
        base64_content = base64.b64encode(xml_bytes).decode('utf-8')
        
        # Define a filename for the XML
        filename = f"{title}.xml"
        
        # Prepare the ContentVersion record
        content_version_data = {
            'Title': title,
            'PathOnClient': filename,
            'VersionData': base64_content,
            'FirstPublishLocationId': library_id
        }
        
        # Upload the file as a ContentVersion
        result = sf.ContentVersion.create(content_version_data)
        success = result.get('success', False)
        
        if success:
            content_version = sf.ContentVersion.get(result.get('id'))
            download_url = content_version.get('VersionDataUrl', 'None')
            
            # Log the success
            integration_log = {
                'Status_Code__c': '200',
                'Message__c': f"Inserted ContentVersion ID: {result.get('id')}",
                'Request_Payload__c': download_url,
                'Response_Payload__c': json.dumps(result),
                'Log_Type__c': 'Python Integration'
            }
        else:
            # Log the error
            integration_log = {
                'Status_Code__c': '500',
                'Message__c': 'Error inserting ContentVersion',
                'Request_Payload__c': 'None',
                'Response_Payload__c': json.dumps(result),
                'Log_Type__c': 'Python Integration'
            }
            raise SalesforceAPIError("Failed to create ContentVersion", 500)

    except Exception as e:
        logger.error(f"Error uploading file to library: {str(e)}")
        # Create error log
        integration_log = {
            'Status_Code__c': '500',
            'Message__c': 'Error inserting ContentVersion',
            'Request_Payload__c': 'None',
            'Response_Payload__c': str(e),
            'Log_Type__c': 'Python Integration'
        }
        # Re-raise with our custom error
        raise SalesforceAPIError(f"Failed to upload file: {str(e)}", 500)
    finally:
        # Always insert the integration log record
        try:
            sf.Integration_Log__c.create(integration_log)
        except Exception as log_error:
            logger.error(f"Failed to create integration log: {str(log_error)}")
    
    return download_url


def generate_salesforce_xml(xml_file_name):
    """
    Generate XML data from Salesforce based on the given file name.
    
    Args:
        xml_file_name (str): Name of the XML file to generate
        
    Returns:
        str: Download URL for the generated XML file
    """
    logger.info(f"Processing XML request for file: {xml_file_name}")
    start_time = time.time()
    
    try:
        # Get cached Salesforce connection
        sf = get_salesforce_connection()
        state_code_map = get_state_code_mapping(sf)
        
        # Query for XML migration settings
        logger.info(f"Querying ABOP_Migration__c records for {xml_file_name}")
        query = f"""
                SELECT Id, Name, Is_Active__c, SOQL_Query_Definition__c, Main_XML_Template__c, 
                      License_XML_Block__c, Address_XML_Block__c, Education_XML_Block__c, 
                      Certification_XML_Block__c, Certificate_XML_Block__c, Issuance_XML_Block__c, 
                      XML_Type__c, XML_Main_Title_Start__c, XML_Main_Title_End__c, 
                      Static_Value_SOQL_Query__c, Object_field_Mapping__c 
                FROM ABOP_Migration__c 
                WHERE Is_Active__c = true 
                AND Name = '{xml_file_name}'
        """
        
        abop_migration_records = sf.query(query)

        if not abop_migration_records["records"]:
            logger.warning(f"No active ABOP_Migration__c records found for {xml_file_name}")
            return "<?xml version='1.0' encoding='utf-8'?><Members></Members>"

        logger.info(f"Found {len(abop_migration_records['records'])} ABOP_Migration__c record(s)")
        record = abop_migration_records["records"][0]
        
        # Parse XML templates
        logger.debug("Parsing XML templates from rich text fields")
        main_xml_header = convert_rich_text_to_plain(record["XML_Main_Title_Start__c"])
        main_xml = convert_rich_text_to_plain(record["Main_XML_Template__c"])
        address_xml = convert_rich_text_to_plain(record["Address_XML_Block__c"])
        education_xml = convert_rich_text_to_plain(record["Education_XML_Block__c"])
        certification_xml = convert_rich_text_to_plain(record["Certification_XML_Block__c"])
        certificate_xml = convert_rich_text_to_plain(record["Certificate_XML_Block__c"])
        license_xml = convert_rich_text_to_plain(record["License_XML_Block__c"])
        issuance_xml = convert_rich_text_to_plain(record["Issuance_XML_Block__c"])
        soql_query = convert_rich_text_to_plain(record["SOQL_Query_Definition__c"])
        static_value_soql_query = convert_rich_text_to_plain(record["Static_Value_SOQL_Query__c"])
        field_mapping = convert_rich_text_to_plain(record["Object_field_Mapping__c"])
        main_xml_footer = record["XML_Main_Title_End__c"]
        logger.debug(f"Parsed field mapping: {field_mapping}")
        
        # Execute Salesforce queries
        logger.info("Executing main SOQL query")
        contact_records = sf.query(soql_query)
        logger.info(f"Retrieved {contact_records.get('totalSize', 0)} contact records")
        
        abo_metadata_records = sf.query(static_value_soql_query)
        logger.info(f"Retrieved {abo_metadata_records.get('totalSize', 0)} metadata records")

        # Replace blocks with their content and create a combined xml
        logger.debug("Assembling XML structure")
        main_xml = main_xml.replace("{{AddressesBlock}}", address_xml)
        main_xml = main_xml.replace("{{EducationsBlock}}", education_xml)
        certificate_xml = certificate_xml.replace("{{IssuancesBlock}}", issuance_xml)
        certification_xml = certification_xml.replace("{{CertificatesBlock}}", certificate_xml)
        main_xml = main_xml.replace("{{CertificationsBlock}}", certification_xml)

        # Generate XML content
        xml_members = generate_xml_members(
            contact_records, 
            abo_metadata_records, 
            main_xml, 
            license_xml,
            field_mapping,
            state_code_map
        )
        xml_member_content = "\n".join(xml_members)
        complete_xml = main_xml_header + "\n" + xml_member_content + "\n" + main_xml_footer

        # Format XML
        logger.debug("Formatting final XML output")
        try:
            parsed_xml = parseString(complete_xml)
            pretty_lines = [line for line in parsed_xml.toprettyxml(indent="   ").split('\n') if line.strip()]
            pretty_xml = '<?xml version="1.0" encoding="utf-8"?>\n' + '\n'.join(pretty_lines[1:])
        except Exception as e:
            logger.error(f"Error formatting XML: {str(e)}")
            pretty_xml = complete_xml  # Fall back to unformatted XML
        
        # Upload to content library
        title = f"{xml_file_name}_{time.strftime('%d-%m-%Y %H.%M.%S')}"
        
        # Find content library ID
        library_result = sf.query("SELECT Id FROM ContentWorkspace WHERE Name = 'ABOP XML files'")
        library_records = library_result.get("records", [])

        if not library_records:
            error_msg = "Content Library 'ABOP XML files' not found"
            logger.error(error_msg)
            raise SalesforceAPIError(error_msg, 500)
            
        library_id = library_records[0]["Id"]
        logger.info(f"Uploading XML file to Salesforce library with ID: {library_id}")
        
        # Upload file to library
        xml_file_download_url = upload_xml_to_library(sf, pretty_xml, title, library_id)
        logger.info(f"XML file uploaded successfully. Download URL: {xml_file_download_url}")

        end_time = time.time()
        logger.info(f"XML generation completed in {end_time - start_time:.2f} seconds")
        return xml_file_download_url

    except SalesforceAPIError as e:
        logger.error(f"Salesforce API error: {str(e)}")
        raise e
    except Exception as e:
        error_msg = f"Error processing XML for {xml_file_name}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise SalesforceAPIError(error_msg, 500)


@app.route('/download', methods=['GET'])
def download_xml():
    """API endpoint to get XML by file name."""
    xml_file_name = request.args.get('xml_file_name')
    
    if not xml_file_name:
        return jsonify({
            "status": "error", 
            "message": "Missing required parameter: xml_file_name"
        }), 400
    
    logger.info(f"Received request for XML file: {xml_file_name}")
    
    try:
        xml_file_download_url = generate_salesforce_xml(xml_file_name)
        logger.info(f"Successfully generated XML for {xml_file_name}")
        return xml_file_download_url

    except SalesforceAPIError as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": e.message,
            "detail": e.detail
        }), e.status_code
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "detail": str(e)
        }), 500


@app.route('/connection', methods=['GET'])
def test_connection():
    """Test the Salesforce connection."""
    try:
        # Test Salesforce connection
        sf = get_salesforce_connection()
        
        # Check if we can query a simple object
        result = sf.query("SELECT Id FROM Account LIMIT 1")
        
        # Return success with some basic info
        return jsonify({
            "status": "success",
            "connection": "established",
            "query_result": f"Found {result.get('totalSize', 0)} records"
        })
    except Exception as e:
        logger.error(f"Connection test error: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500


@app.route("/")
def health_check():
    """Simple health check endpoint."""
    return "Salesforce XML API is running"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "False").lower() == "true"
    
    logger.info(f"Starting Salesforce XML API server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)