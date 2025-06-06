import requests
import csv
import time
import os
import traceback
import json
from datetime import datetime

# Configuration
BASE_URL = "https://www.base.gov.pt/Base4/pt/resultados/"
MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 120
MAX_RUNTIME_MINUTES = 40
PROGRESS_FILE = 'details_extraction_progress.json'
INPUT_FILE = 'contractsfinal.csv'
OUTPUT_FILE = 'contractsfinal_plus.csv'
PAGE_SIZE = 50

headers = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.base.gov.pt",
    "Connection": "keep-alive",
    "Referer": "https://www.base.gov.pt/Base4/pt/resultados/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

def create_session():
    """Create a session with browser-like headers"""
    session = requests.Session()
    session.headers.update(headers)
    return session

def fetch_contracts_page(session, page_number):
    """Fetch a single page of contracts"""
    data = {
        "type": "search_contratos",
        "version": "133.0",
        "query": "cpv=09123000",  # Natural gas CPV code
        "sort": "-publicationDate",
        "page": str(page_number),
        "size": str(PAGE_SIZE)
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching contracts page {page_number + 1} (attempt {attempt + 1})...")
            response = session.post(BASE_URL, data=data, timeout=TIMEOUT)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    print(f"Response preview: {str(json_data)[:200]}...")
                    if json_data and 'items' in json_data:
                        return json_data
                    else:
                        print(f"Invalid response format for page {page_number + 1}")
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON response: {str(e)}")
                    print(f"Raw response content: {response.text[:500]}...")
            
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
    
    return None

def fetch_contract_details(session, contract_id):
    """Fetch detailed information for a single contract"""
    # Request for contract details
    data = {
        "type": "detail_contratos",  # Correct type for contract details
        "version": "133.0",  # Correct version for details endpoint
        "id": str(contract_id)
    }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching details for contract {contract_id} (attempt {attempt + 1})...")
            print(f"Request data: {data}")
            
            # Add a longer delay between retries to handle rate limiting
            if attempt > 0:
                delay = RETRY_DELAY * (attempt + 1)  # Progressive delay
                print(f"Waiting {delay} seconds before retry...")
                time.sleep(delay)
            
            response = session.post(
                BASE_URL,
                data=data,
                timeout=TIMEOUT,
                allow_redirects=True
            )
            
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            print(f"Request headers sent: {dict(response.request.headers)}")
            
            if response.status_code == 200:
                try:
                    # Print raw response first
                    content = response.text
                    print(f"Raw response content: {content[:500]}...")
                    
                    if not content.strip():
                        print("Empty response received")
                        if attempt < MAX_RETRIES - 1:
                            print("Retrying due to empty response...")
                            continue
                        return None
                    
                    json_data = response.json()
                    print(f"Parsed JSON preview: {str(json_data)[:200]}...")
                    
                    # Verify we got a valid contract detail object with all required fields
                    if json_data and isinstance(json_data, dict) and 'id' in json_data:
                        # Ensure all fields are present, even if null
                        required_fields = [
                            'documents', 'invitees', 'publicationDate', 'observations', 'ccp',
                            'totalEffectivePrice', 'endOfContractType', 'contractingProcedureUrl',
                            'ambientCriteria', 'directAwardFundamentationType', 'announcementId',
                            'contestants', 'frameworkAgreementProcedureId', 
                            'frameworkAgreementProcedureDescription', 'contractFundamentationType',
                            'increments', 'closeDate', 'causesDeadlineChange', 'causesPriceChange',
                            'executionDeadline', 'contractingProcedureType', 'contractTypeCS',
                            'executionPlace', 'centralizedProcedure', 'cpvs', 'objectBriefDescription',
                            'income', 'nonWrittenContractJustificationTypes', 'initialContractualPrice',
                            'contractStatus', 'materialCriteria', 'contractTypes', 'signingDate',
                            'cpvsValue', 'contracted', 'normal', 'contracting', 'cocontratantes',
                            'aquisitionStateMemberUE', 'infoAquisitionStateMemberUE', 'groupMembers',
                            'specialMeasures', 'regime', 'cpvsType', 'cpvsDesignation', 'description',
                            'id'
                        ]
                        
                        # Initialize missing fields with null
                        for field in required_fields:
                            if field not in json_data:
                                json_data[field] = None
                                
                        print(f"Successfully retrieved details for contract {contract_id}")
                        return json_data
                    else:
                        print(f"Invalid response format for contract {contract_id}")
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON response: {str(e)}")
                    print(f"Full raw response content: {content}")
            elif response.status_code == 429:  # Too Many Requests
                print("Rate limit hit, waiting longer before retry...")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * 5)  # Wait 5 times longer
                    continue
            else:
                print(f"Request failed with status {response.status_code}")
                print(f"Error response: {response.text[:500]}...")
            
        except requests.exceptions.RequestException as e:
            print(f"Request error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print("Retrying...")
                continue
    
    return None

def save_progress(current_index, total_processed):
    """Save the current progress to a file"""
    progress = {
        'current_index': current_index,
        'total_processed': total_processed,
        'timestamp': datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

def load_progress():
    """Load the last saved progress"""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
            return progress['current_index'], progress['total_processed']
    except Exception as e:
        print(f"Error loading progress: {e}")
    return 0, 0

def append_to_csv(index, page, contract_id, contract_details):
    """Append contract details to CSV file, keeping original index, page and id"""
    try:
        file_exists = os.path.exists(OUTPUT_FILE)
        mode = 'a' if file_exists else 'w'
        
        with open(OUTPUT_FILE, mode, newline='', encoding='utf-8') as csvfile:
            # Define the fields we want to extract, starting with original fields
            fieldnames = [
                'index',
                'page',
                'id',
                'documents',
                'invitees',
                'publicationDate',
                'observations',
                'ccp',
                'totalEffectivePrice',
                'endOfContractType',
                'contractingProcedureUrl',
                'ambientCriteria',
                'directAwardFundamentationType',
                'announcementId',
                'contestants',
                'frameworkAgreementProcedureId',
                'frameworkAgreementProcedureDescription',
                'contractFundamentationType',
                'increments',
                'closeDate',
                'causesDeadlineChange',
                'causesPriceChange',
                'executionDeadline',
                'contractingProcedureType',
                'contractTypeCS',
                'executionPlace',
                'centralizedProcedure',
                'cpvs',
                'objectBriefDescription',
                'income',
                'nonWrittenContractJustificationTypes',
                'initialContractualPrice',
                'contractStatus',
                'materialCriteria',
                'contractTypes',
                'signingDate',
                'cpvsValue',
                'contracted',
                'normal',
                'contracting',
                'cocontratantes',
                'aquisitionStateMemberUE',
                'infoAquisitionStateMemberUE',
                'groupMembers',
                'specialMeasures',
                'regime',
                'cpvsType',
                'cpvsDesignation',
                'description'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            # Create a new row with original fields and contract details
            row_data = {
                'index': index,
                'page': page,
                'id': contract_id
            }
            
            # Add the rest of the contract details
            for field in fieldnames[3:]:  # Skip index, page, id as we already added them
                value = contract_details.get(field)
                if isinstance(value, (list, dict)):
                    row_data[field] = json.dumps(value, ensure_ascii=False)
                else:
                    row_data[field] = value
            
            writer.writerow(row_data)
        return True
    except Exception as e:
        print(f"Error appending to CSV: {str(e)}")
        traceback.print_exc()
        return False

def main():
    try:
        print("Starting contract details extraction with progress tracking...")
        print(f"Current working directory: {os.getcwd()}")
        
        # Load previous progress if any
        start_index, total_processed = load_progress()
        print(f"Resuming from index {start_index} (total processed: {total_processed})")
        
        session = create_session()
        start_time = datetime.now()
        
        # Read contracts from the input CSV file
        with open(INPUT_FILE, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row
            
            for i, row in enumerate(reader):
                # Check if we should skip this row based on progress
                if i < start_index:
                    continue
                
                # Check if we've been running too long
                runtime = datetime.now() - start_time
                if runtime.total_seconds() > MAX_RUNTIME_MINUTES * 60:
                    print(f"\nScript has been running for over {MAX_RUNTIME_MINUTES} minutes.")
                    print(f"Stopping at index {i}")
                    print("Please run the script again to continue from this point.")
                    save_progress(i, total_processed)
                    return
                
                try:
                    # Extract index, page and contract_id from the row
                    index = row[0]
                    page = row[1]
                    contract_id = row[2]
                    
                    print(f"\nProcessing contract {contract_id} (Index: {index}, Page: {page})")
                    
                    # Fetch detailed information using the contract ID
                    contract_details = fetch_contract_details(session, contract_id)
                    
                    if contract_details:
                        # Save the details to the CSV, including original index and page
                        if not append_to_csv(index, page, contract_id, contract_details):
                            print(f"Failed to save details for contract {contract_id}. Stopping here.")
                            save_progress(i, total_processed)
                            return
                        
                        total_processed += 1
                        save_progress(i + 1, total_processed)
                        print(f"Successfully processed contract {contract_id}")
                        print(f"Total contracts processed: {total_processed}")
                    else:
                        print(f"Failed to fetch details for contract {contract_id}. Stopping here.")
                        save_progress(i, total_processed)
                        return
                    
                    # Add delay between requests to avoid rate limiting
                    time.sleep(RETRY_DELAY)
                    
                except Exception as e:
                    print(f"Error processing row {i}: {str(e)}")
                    traceback.print_exc()
                    save_progress(i, total_processed)
                    return
        
        print(f"\n{'='*50}")
        print(f"EXTRACTION COMPLETED SUCCESSFULLY!")
        print(f"{'='*50}")
        print(f"Total contracts processed: {total_processed}")
        
        # Remove progress file since we're done
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            
    except Exception as e:
        print(f"Critical error: {str(e)}")
        traceback.print_exc()
        print("\nSaving progress before exit...")
        save_progress(start_index, total_processed)

if __name__ == "__main__":
    main()