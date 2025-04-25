from fastapi import FastAPI, HTTPException, Response
from simple_salesforce import Salesforce
from xml.dom.minidom import parseString
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import uvicorn
import html
import os
import json
import logging
from functools import lru_cache
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("salesforce_xml_api")

# Load environment variables
# load_dotenv(dotenv_path="creds.env")

app = FastAPI(
    title="Salesforce XML API", 
    description="API to generate XML files from Salesforce"
)

# Cache for Salesforce connection to avoid repeated authentication
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
        logger.error(f"Failed to connect to Salesforce: {str(e)}")
        raise

def parse_rich_text_to_compact_xml(html_data: str) -> str:
    """Parse rich text HTML to plain text with compact XML formatting."""
    if not html_data:
        return ""
    
    soup = BeautifulSoup(html_data, "html.parser")
    lines = [
        html.unescape(p.get_text()).replace("\xa0", " ") for p in soup.find_all("p")
    ]
    return "\n".join(lines).strip()

def generate_xml_members(sf_query_result, metadata_result, main_xml_template, license_xml_template, fieldMapping):
    """Generate XML members from Salesforce query results."""
    logger.info("Generating XML members")
    start_time = time.time()
    xml_members = []
    
    if isinstance(fieldMapping, str):
        try:
            # Assuming fieldMapping string is in JSON format
            fieldMapping = json.loads(fieldMapping)
        except Exception as e:
            logger.error(f"Failed to parse fieldMapping as JSON: {e}")
            # Create empty dictionary to avoid errors
            fieldMapping = {}

    # Null check for query results
    if not sf_query_result or 'records' not in sf_query_result or not sf_query_result['records']:
        logger.warning("No contact records found in query result")
        return xml_members
    
    contact_records = sf_query_result.get('records', [])
    logger.info(f"Processing {len(contact_records)} contact records")
    
    # Null check for metadata
    if not metadata_result or 'records' not in metadata_result or not metadata_result['records']:
        logger.warning("No metadata records found")
        metadata_record = {}
    else:
        metadata_record = metadata_result['records'][0]
        logger.info(f"Using metadata record: {metadata_record.get('Id')}")
    
    # Process each contact
    for idx, contact in enumerate(contact_records):
        if not contact:  # Skip if contact is None or empty
            logger.warning(f"Skipping empty contact at index {idx}")
            continue
            
        # Log processing for debugging
        logger.debug(f"Processing contact {idx+1}/{len(contact_records)}: {contact.get('Id')}")
        
        # Generate license blocks for this contact
        license_blocks = []
        
        # Proper null check for licenses
        medical_licenses = contact.get('Medical_Licenses__r')
        if medical_licenses and 'records' in medical_licenses:
            licenses = medical_licenses.get('records', [])
            
            if licenses:  # Check if licenses list is not empty
                logger.debug(f"Processing {len(licenses)} licenses for contact {contact.get('Id')}")
                for license_idx, license in enumerate(licenses):
                    if not license:  # Skip if license is None or empty
                        logger.warning(f"Skipping empty license at index {license_idx} for contact {contact.get('Id')}")
                        continue
                        
                    # Replace placeholders in license XML template
                    license_block = license_xml_template
                    
                    # Using fieldMapping for license fields
                    for placeholder, field_name in fieldMapping.items():
                        if placeholder.startswith("{{Medical_License__c."):
                            field = placeholder.split(".")[-1].rstrip("}}")
                            value = license.get(field)
                            # Convert value to string if it's not None
                            value_str = str(value) if value is not None else ''
                            license_block = license_block.replace(placeholder, value_str)
                    
                    license_blocks.append(license_block)
        
        # Join all license blocks into a single string
        licenses_block = "\n".join(license_blocks)
        
        # Create a copy of the main XML template for this contact
        contact_xml = main_xml_template
        
        # Replace the licenses block placeholder
        contact_xml = contact_xml.replace("{{LicensesBlock}}", licenses_block)
        
        # Handle special cases first
        # Handle MailingAddress specially since it's a nested structure
        mailing_address = contact.get('MailingAddress')
        if mailing_address:
            # Handle both string and OrderedDict types
            if isinstance(mailing_address, dict):
                street = mailing_address.get('street', '')
                address_str = str(street)
            else:
                address_str = str(mailing_address)
            contact_xml = contact_xml.replace("{{Contact.MailingAddress}}", address_str)
        else:
            contact_xml = contact_xml.replace("{{Contact.MailingAddress}}", "")
        
        # Replace all other placeholders using the field mapping
        for placeholder, field_name in fieldMapping.items():
            # Skip blocks and already handled special cases
            if placeholder in ["{{LicensesBlock}}", "{{AddressesBlock}}", "{{EducationsBlock}}", 
                              "{{CertificationsBlock}}", "{{IssuancesBlock}}", "{{Contact.MailingAddress}}"]:
                continue
                
            if placeholder.startswith("{{Contact."):
                value = contact.get(field_name)
                # Convert value to string if it's not None
                if value is not None:
                    if isinstance(value, (dict, list)):
                        value_str = str(value)
                    else:
                        value_str = str(value)
                else:
                    value_str = ''
                contact_xml = contact_xml.replace(placeholder, value_str)
            elif placeholder.startswith("{{ABO_Setting__mdt."):
                value = metadata_record.get(field_name)
                # Convert value to string if it's not None
                value_str = str(value) if value is not None else ''
                contact_xml = contact_xml.replace(placeholder, value_str)
            elif placeholder == "{{Salesforce.DataMappingPending}}":
                contact_xml = contact_xml.replace(placeholder, "Pending")
        
        xml_members.append(contact_xml)
    
    end_time = time.time()
    logger.info(f"XML member generation completed in {end_time - start_time:.2f} seconds")
    return xml_members

def get_salesforce_xml(xml_file_name: str) -> str:
    """Get XML data from Salesforce based on the given file name."""
    logger.info(f"Processing XML request for file: {xml_file_name}")
    start_time = time.time()
    
    try:
        # Get cached Salesforce connection
        sf = get_salesforce_connection()

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
        
        abopMigration_records = sf.query(query)

        if not abopMigration_records["records"]:
            logger.warning(f"No active ABOP_Migration__c records found for {xml_file_name}")
            return "<?xml version='1.0' encoding='utf-8'?><Members/>"

        logger.info(f"Found {len(abopMigration_records['records'])} ABOP_Migration__c record(s)")
        record = abopMigration_records["records"][0]
        
        # Parse XML templates
        logger.debug("Parsing XML templates from rich text fields")
        main_xml_header = parse_rich_text_to_compact_xml(record["XML_Main_Title_Start__c"])
        main_xml = parse_rich_text_to_compact_xml(record["Main_XML_Template__c"])
        address_xml = parse_rich_text_to_compact_xml(record["Address_XML_Block__c"])
        education_xml = parse_rich_text_to_compact_xml(record["Education_XML_Block__c"])
        certification_xml = parse_rich_text_to_compact_xml(record["Certification_XML_Block__c"])
        certificate_xml = parse_rich_text_to_compact_xml(record["Certificate_XML_Block__c"])
        license_xml = parse_rich_text_to_compact_xml(record["License_XML_Block__c"])
        issuance_xml = parse_rich_text_to_compact_xml(record["Issuance_XML_Block__c"])
        soql_query = parse_rich_text_to_compact_xml(record["SOQL_Query_Definition__c"])
        static_value_soql_query = parse_rich_text_to_compact_xml(record["Static_Value_SOQL_Query__c"])
        fieldMapping = parse_rich_text_to_compact_xml(record["Object_field_Mapping__c"])
        main_xml_footer = record["XML_Main_Title_End__c"]
        print(f"fieldMapping : {fieldMapping}")
        
        # Log the queries for debugging
        logger.info(f"Executing main SOQL query")
        logger.debug(f"Main SOQL query: {soql_query}")
        logger.debug(f"Static value SOQL query: {static_value_soql_query}")
        
        # Execute Salesforce queries
        contact_records = sf.query(soql_query)
        logger.info(f"Retrieved {contact_records.get('totalSize', 0)} contact records")
        
        abo_metadata_records = sf.query(static_value_soql_query)
        logger.info(f"Retrieved {abo_metadata_records.get('totalSize', 0)} metadata records")

        # Replace block with their block and create a combined xml
        logger.debug("Assembling XML structure")
        main_xml = main_xml.replace("{{AddressesBlock}}", address_xml)
        main_xml = main_xml.replace("{{EducationsBlock}}", education_xml)
        certificate_xml = certificate_xml.replace("{{IssuancesBlock}}", issuance_xml)
        certification_xml = certification_xml.replace("{{CertificatesBlock}}", certificate_xml)
        main_xml = main_xml.replace("{{CertificationsBlock}}", certification_xml)

        # Generate XML content
        xml_members = generate_xml_members(contact_records, abo_metadata_records, main_xml, license_xml,fieldMapping)
        xml_member = "\n".join(xml_members)
        xml_member = main_xml_header + "\n" + xml_member + "\n" + main_xml_footer

        # Format XML
        logger.debug("Formatting final XML output")
        parsed_xml = parseString(xml_member)
        pretty_body = '\n'.join([line for line in parsed_xml.toprettyxml(indent="   ").split('\n') if line.strip()])
        pretty_xml = '<?xml version="1.0" encoding="utf-8"?>\n' + '\n'.join(pretty_body.split('\n')[1:])
        
        end_time = time.time()
        logger.info(f"XML generation completed in {end_time - start_time:.2f} seconds")
        return pretty_xml

    except Exception as e:
        logger.error(f"Error processing XML for {xml_file_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing XML: {str(e)}")


@app.get("/")
def get_xml(xml_file_name: str):
    """API endpoint to get XML by file name."""
    logger.info(f"Received request for XML file: {xml_file_name}")
    try:
        xml_content = get_salesforce_xml(xml_file_name)
        logger.info(f"Successfully generated XML for {xml_file_name}")
        return Response(
            content=xml_content,
            media_type="application/xml",
            headers={"Content-Disposition": f"attachment; filename={xml_file_name}.xml"},
        )
    except HTTPException as e:
        logger.error(f"HTTP exception occurred: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {str(e)}"
        )

if __name__ == "__main__":
    logger.info("Starting Salesforce XML API server")
    uvicorn.run(app, host="0.0.0.0", port=8000)