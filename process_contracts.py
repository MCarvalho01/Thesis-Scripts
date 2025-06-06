import requests
import csv
import time
import os
import traceback
import json
from datetime import datetime

# Configuration
BASE_URL = "https://www.base.gov.pt/Base4/pt/resultados/"
PAGE_SIZE = 50
MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 120
MAX_RUNTIME_MINUTES = 40  # Maximum runtime before stopping
PROGRESS_FILE = 'extraction_progress.json'
OUTPUT_FILE = 'contractsfinal.csv'

headers = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.base.gov.pt",
    "Connection": "keep-alive",
    "Referer": "https://www.base.gov.pt/Base4/pt/resultados/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
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
        "version": "114.0",
        "query": "cpv=09123000",  # Simplified query to focus on natural gas CPV code
        "sort": "-publicationDate",
        "page": str(page_number),
        "size": str(PAGE_SIZE)
    }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching page {page_number + 1} (attempt {attempt + 1})...")
            print(f"Request data: {data}")
            
            # Make the POST request
            response = session.post(BASE_URL, data=data, timeout=TIMEOUT)
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    # Try to parse JSON
                    json_data = response.json()
                    total = json_data.get('total', 0)
                    items = json_data.get('items', [])
                    
                    # Verify total number of contracts on first page
                    if page_number == 0 and total != 2146:
                        print(f"Error: Expected 2,146 total contracts but got {total}")
                        return None
                    
                    # Print debug information about the response
                    print(f"Successfully parsed JSON response:")
                    print(f"- Total items in response: {total}")
                    print(f"- Items on this page: {len(items)}")
                    if items:
                        print(f"- First item preview: {json.dumps(items[0], indent=2)[:200]}...")
                    
                    return json_data
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON response: {str(e)}")
                    print(f"Response content preview: {response.text[:200]}...")
            else:
                print(f"Request failed with status {response.status_code}")
                print(f"Error response: {response.text[:200]}...")
            
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            
        except requests.exceptions.RequestException as e:
            print(f"Request error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
    
    return None

def clean_price(price_str):
    """Clean price string by removing € symbol and converting to float"""
    try:
        if not price_str or price_str == '0,00 €':
            return 0.0
        return float(price_str.replace('€', '').replace('.', '').replace(',', '.').strip())
    except (ValueError, AttributeError) as e:
        print(f"Error cleaning price {price_str}: {str(e)}")
        return 0.0

def save_progress(last_page, total_contracts):
    """Save the current progress to a file"""
    progress = {
        'last_page': last_page,
        'total_contracts': total_contracts,
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
            return progress['last_page'], progress['total_contracts']
    except Exception as e:
        print(f"Error loading progress: {e}")
    return 0, 0

def append_to_csv(items, start_index):
    """Append contracts to CSV file with index tracking"""
    try:
        file_exists = os.path.exists(OUTPUT_FILE)
        mode = 'a' if file_exists else 'w'
        
        with open(OUTPUT_FILE, mode, newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'index', 'page', 'id', 'contracting', 'contracted', 
                'contractingProcedureType', 'publicationDate', 
                'initialContractualPrice', 'signingDate',
                'objectBriefDescription', 'ccp'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            
            for i, item in enumerate(items):
                contract_data = {
                    'index': start_index + i,
                    'page': start_index // PAGE_SIZE,
                    'id': item.get('id', ''),
                    'contracting': item.get('contracting', ''),
                    'contracted': item.get('contracted', ''),
                    'contractingProcedureType': item.get('contractingProcedureType', ''),
                    'publicationDate': item.get('publicationDate', ''),
                    'initialContractualPrice': clean_price(item.get('initialContractualPrice', '0')),
                    'signingDate': item.get('signingDate', ''),
                    'objectBriefDescription': item.get('objectBriefDescription', ''),
                    'ccp': item.get('ccp', False)
                }
                writer.writerow(contract_data)
        
        return True
    except Exception as e:
        print(f"Error appending to CSV: {str(e)}")
        traceback.print_exc()
        return False

def main():
    try:
        print("Starting contract extraction with progress tracking...")
        print(f"Current working directory: {os.getcwd()}")
        
        # Load previous progress if any
        start_page, total_contracts = load_progress()
        print(f"Resuming from page {start_page} (total contracts so far: {total_contracts})")
        
        session = create_session()
        expected_total = 2146
        expected_pages = 43  # 2146/50 rounded up
        start_time = datetime.now()
        
        # Verify we can still get the correct total
        first_page = fetch_contracts_page(session, 0)
        if not first_page:
            print("Failed to fetch first page or incorrect total number of contracts")
            return
        
        total_items = first_page.get('total', 0)
        if total_items != expected_total:
            print(f"Error: Got {total_items} total items, expected {expected_total}")
            return
        
        # Process remaining pages
        for page in range(start_page, expected_pages):
            # Check if we've been running too long
            runtime = datetime.now() - start_time
            if runtime.total_seconds() > MAX_RUNTIME_MINUTES * 60:
                print(f"\nScript has been running for over {MAX_RUNTIME_MINUTES} minutes.")
                print(f"Stopping at page {page} with {total_contracts} contracts collected.")
                print("Please run the script again to continue from this point.")
                save_progress(page, total_contracts)
                return
            
            print(f"\n{'='*50}")
            print(f"PROGRESS: Page {page + 1} of {expected_pages} ({((page+1)/expected_pages)*100:.1f}% complete)")
            print(f"Total contracts so far: {total_contracts}")
            print(f"{'='*50}")
            
            page_data = fetch_contracts_page(session, page)
            if page_data and 'items' in page_data:
                items = page_data['items']
                if not append_to_csv(items, total_contracts):
                    print(f"Failed to save page {page + 1}. Stopping here.")
                    save_progress(page, total_contracts)
                    return
                
                total_contracts += len(items)
                save_progress(page + 1, total_contracts)
                
                print(f"Retrieved and saved {len(items)} contracts from page {page + 1}")
                print(f"Total contracts collected: {total_contracts} of {expected_total}")
                
                if total_contracts >= expected_total:
                    print("\nAll contracts have been collected!")
                    break
            else:
                print(f"Failed to fetch page {page + 1}. Stopping here.")
                save_progress(page, total_contracts)
                return
            
            time.sleep(RETRY_DELAY)
        
        if total_contracts < expected_total:
            print(f"\nWarning: Only collected {total_contracts} contracts out of {expected_total}")
            print("Please run the script again to collect the remaining contracts")
        else:
            print(f"\n{'='*50}")
            print(f"EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"{'='*50}")
            print(f"Total contracts collected: {total_contracts}")
            # Remove progress file since we're done
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
        
    except Exception as e:
        print(f"Critical error: {str(e)}")
        traceback.print_exc()
        print("\nSaving progress before exit...")
        save_progress(start_page, total_contracts)

if __name__ == "__main__":
    main() 