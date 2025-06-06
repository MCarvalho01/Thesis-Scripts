# Add these at the top (before any functions)
import csv
import sys
import requests
import time
import string
import random  # Add this import
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import os
import re
import json
from datetime import datetime
import base64

# Add Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configuration constants
REQUEST_DELAY = 1
REQUEST_TIMEOUT = 10
SELENIUM_TIMEOUT = 30
CSV_URL_FIELD = "contractingProcedureUrl"  # Verify this matches your CSV column name
BASE_DOMAIN = "https://community.vortal.biz/Public"
DOWNLOAD_ENDPOINT = "/archive/api/PublicDownload/download"
PROCEDURAL_SECTION_NAME = "Documentos Disponíveis"  # Updated to Portuguese

# Cookie configuration
INITIAL_COOKIES = {
    'HAPRXSIDCOM': '10.101.2.12:443',
    '.AspNetCore.Session': 'CfDJ8JiXOVDWYq9Gocj9m9iRecHMs1ndx9yZsh3uYg3vzrJbHBLfR9b5E%2BExGkpvhJs2mf5CRtq3WtHBgSYVVd4oyn7beVbAdbus%2BbC94xi1tDKW7rNBzyPQCVFwc7PgY0wW0H4VQoU8rpIh5pJSBW%2BFJc9SvH2sUd%2FoAKzQC5LckCe5',
    'CompanyCode': '703196428',
    'FirstLoginMade': 'True',
    'NextVision': 'chunks-2',
    'VisionNextCookie': 'oAe3M0n6j56rmgug5relC%2Fv0pXXtyg7KRrpRtJ97yamErcYbh7R4tGuknq20ryBm',
    'NextVisionC1': 'CfDJ8JiXOVDWYq9Gocj9m9iRecFdIO-qSa4QHOLssfXDNlqdSe4o8Q_CSGHuO979WIA8G9r5WH1VhNbYRNPfrh91Puf8p88Ub34ff_gsQL4zpWVvYDWuz1FX-sxg1F50gaYNfvvSVcgkKnz5ouaEMOVcjpc4fEh4knQtXo-oH1Zsjv5_SP6HAhr-kuoHF7fLKfvww0EhHoQpKHyfP8LZ9xfx15W7PXHHcuEGlS4bU8sdkv87TRu141VOtzLvjs80KFXBRFHwkGgDYup4iJi_5eI5vvK3ZKS4B7BahmSu8H7o2RD81eAuxtB0dL_kfV_lI69gfG09NeutC_0oLhD_oHSqMv4'
}

# Track failed URLs
failed_urls = []

# List of User-Agents to rotate through
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

class DownloadTracker:
    def __init__(self):
        self.incomplete_downloads = []
        self.progress_file = "download_progress.json"
        self.error_file = "download_errors.json"
        
    def add_incomplete_download(self, url, cpv, doc_id, total_docs, downloaded_docs):
        self.incomplete_downloads.append({
            'url': url,
            'cpv': cpv,
            'doc_id': doc_id,
            'total_documents': total_docs,
            'downloaded_documents': downloaded_docs,
            'timestamp': datetime.now().isoformat()
        })
        
    def save_progress(self, processed_urls):
        progress = {
            'processed_urls': list(processed_urls),
            'timestamp': datetime.now().isoformat()
        }
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
            
    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                return set(progress['processed_urls'])
            except:
                return set()
        return set()
        
    def save_error_report(self):
        report = {
            'incomplete_downloads': self.incomplete_downloads,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.error_file, 'w') as f:
            json.dump(report, f, indent=2)
            
    def print_summary(self):
        """Print a detailed summary of download results"""
        print("\n" + "="*50)
        print("DOWNLOAD SUMMARY REPORT")
        print("="*50)
        
        if not self.incomplete_downloads and not failed_urls:
            print("\n✓ All documents were downloaded successfully!")
            return
            
        # Failed URLs (no documents downloaded)
        if failed_urls:
            print("\nFAILED EXTRACTIONS (No documents downloaded):")
            print("-"*40)
            for url in failed_urls:
                print(f"❌ {url}")
                
        # Partial downloads
        partial_downloads = [entry for entry in self.incomplete_downloads 
                           if entry['downloaded_documents'] > 0]
        if partial_downloads:
            print("\nPARTIAL DOWNLOADS:")
            print("-"*40)
            for entry in partial_downloads:
                print(f"\n⚠️  {entry['url']}")
                print(f"   CPV: {entry['cpv']}")
                print(f"   ID: {entry['doc_id']}")
                print(f"   Downloaded: {entry['downloaded_documents']}/{entry['total_documents']} documents")
                success_rate = (entry['downloaded_documents'] / entry['total_documents']) * 100
                print(f"   Success Rate: {success_rate:.1f}%")
                
        # Summary statistics
        print("\nSTATISTICS:")
        print("-"*40)
        print(f"Total failed URLs: {len(failed_urls)}")
        print(f"Total partial downloads: {len(partial_downloads)}")
        total_missing = sum(entry['total_documents'] - entry['downloaded_documents'] 
                          for entry in self.incomplete_downloads)
        print(f"Total missing documents: {total_missing}")
        
        print("\nNOTE: This information has been saved to 'download_errors.json' for reference.")

# Create global tracker instance
download_tracker = DownloadTracker()

def get_random_headers():
    """Generate random headers for requests"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-PT,pt;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }

def make_request(session, url, stream=False, retry_count=0):
    """Make a request with retry logic and delay"""
    try:
        # Update headers before request
        session.headers.update(get_random_headers())
        
        # Add delay between requests
        delay = REQUEST_DELAY * (1 + random.random())
        print(f"Waiting {delay:.1f} seconds before request...")
        time.sleep(delay)
        
        response = session.get(url, stream=stream)
        
        # Add validation
        if response.status_code != 200:
            print(f"Invalid status code: {response.status_code}")
            raise requests.exceptions.RequestException(f"Status code {response.status_code}")
            
        if 'text/html' in response.headers.get('Content-Type', '') and '/PRODPublic/' in url:
            print("Received HTML content instead of document")
            raise requests.exceptions.RequestException("HTML content received for document request")
            
        response.raise_for_status()
        return response
        
    except requests.exceptions.RequestException as e:
        if retry_count < MAX_RETRIES:
            print(f"Request failed, retrying ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(REQUEST_DELAY * 2)  # Double delay on retry
            return make_request(session, url, stream, retry_count + 1)
        raise e

def is_valid_url(url):
    """Check if URL starts with the required domain"""
    return url.startswith(BASE_DOMAIN)

def sanitize_filename(name):
    """Clean filenames for safe filesystem use"""
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    cleaned = ''.join(c for c in name if c in valid_chars)
    return cleaned.strip().rstrip('.')  # Remove trailing dots

def get_unique_filename(filename):
    """Prevent file overwrites"""
    base, ext = os.path.splitext(filename)
    counter = 0
    while os.path.exists(filename):
        counter += 1
        filename = f"{base}_{counter}{ext}"
    return filename

def extract_download_params(onclick_attr):
    """Extract document ID and mkey from onclick attribute"""
    try:
        if not onclick_attr:
            return None, None
            
        # Extract parameters from the onclick JavaScript
        doc_id_match = re.search(r"documentFileId='\s*(\d+)\s*'", onclick_attr) or re.search(r"documentFileId=(\d+)", onclick_attr)
        mkey_match = re.search(r"mkey=([^'&,\s]+)", onclick_attr)
        
        if doc_id_match and mkey_match:
            return doc_id_match.group(1), mkey_match.group(1)
            
    except Exception as e:
        print(f"Error extracting parameters: {str(e)}")
    return None, None

def extract_documents(driver, url, cpv, doc_id):
    """Extract documents using Selenium WebDriver"""
    try:
        print(f"\nFetching page: {url}")
        
        # Navigate to the page
        try:
            driver.get(url)
            WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except Exception as e:
            print(f"Failed to load page: {str(e)}")
            failed_urls.append(url)
            return False
            
        # Wait for document links to load
        time.sleep(3)  # Allow dynamic content to load
        
        # First, try to find the "Documentos Disponíveis" section
        try:
            # Try different methods to locate the section
            section = None
            
            # Method 1: Try to find by heading text
            section_headers = driver.find_elements(By.XPATH, 
                f"//h2[contains(text(), '{PROCEDURAL_SECTION_NAME}')] | " +
                f"//h3[contains(text(), '{PROCEDURAL_SECTION_NAME}')] | " +
                f"//h4[contains(text(), '{PROCEDURAL_SECTION_NAME}')]")
            
            if section_headers:
                # Get the parent section/div containing the documents
                section = section_headers[0].find_element(By.XPATH, "./following-sibling::div[1]")
            
            # Method 2: Try to find by section/div with a specific class that contains the text
            if not section:
                section_elements = driver.find_elements(By.XPATH,
                    f"//*[contains(@class, 'section') or contains(@class, 'documents')]" +
                    f"[contains(., '{PROCEDURAL_SECTION_NAME}')]")
                if section_elements:
                    section = section_elements[0]
            
            # If section found, look for download links within it
            if section:
                download_links = section.find_elements(By.CSS_SELECTOR, "a.buttonDownload")
            else:
                # Fallback: search in entire page
                print(f"Warning: Could not find '{PROCEDURAL_SECTION_NAME}' section, searching entire page")
                download_links = driver.find_elements(By.CSS_SELECTOR, "a.buttonDownload")
        except Exception as e:
            print(f"Error finding document section: {str(e)}")
            download_links = driver.find_elements(By.CSS_SELECTOR, "a.buttonDownload")
            
        if not download_links:
            print(f"No download links found in {url}")
            failed_urls.append(url)
            return False
            
        print(f"Found {len(download_links)} potential document links")
        
        # Validate download links before creating directory
        valid_downloads = []
        for link in download_links:
            href = link.get_attribute("href")
            if href and "token=" in href:
                doc_name = link.text.strip()
                if not doc_name:
                    try:
                        token = href.split("token=")[1]
                        decoded = base64.b64decode(token).decode('utf-8')
                        parts = decoded.split('#')
                        if len(parts) > 1:
                            doc_name = parts[1]
                    except Exception as e:
                        timestamp = int(time.time())
                        doc_name = f"document_{timestamp}"
                valid_downloads.append((link, doc_name, href))
        
        if not valid_downloads:
            print("No valid download links found")
            failed_urls.append(url)
            return False
            
        # Create output directory only if we have valid downloads
        output_dir = f"{sanitize_filename(cpv)}_{sanitize_filename(doc_id)}"
        output_dir_path = os.path.join(os.getcwd(), output_dir)
        os.makedirs(output_dir_path, exist_ok=True)
        
        # Update download directory for Chrome
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": os.path.abspath(output_dir_path)
            }
        )
        
        # Get list of existing files
        existing_files = set(os.listdir(output_dir_path))
        
        # Process each validated download
        success_count = 0
        for link, doc_name, href in valid_downloads:
            try:
                # Clean up filename and ensure it has an extension
                filename = sanitize_filename(doc_name)
                if not os.path.splitext(filename)[1]:
                    filename += '.pdf'
                
                # Check if file already exists
                if filename in existing_files:
                    print(f"File already exists: {filename}")
                    success_count += 1
                    continue
                
                print(f"\nDownloading: {filename}")
                
                # Clear any existing .crdownload or .tmp files
                for temp_file in os.listdir(output_dir_path):
                    if temp_file.endswith(('.crdownload', '.tmp')):
                        try:
                            os.remove(os.path.join(output_dir_path, temp_file))
                        except:
                            pass
                
                # Get initial file list
                initial_files = set(os.listdir(output_dir_path))
                
                # Click the download link
                try:
                    # Open download URL in a new tab
                    original_window = driver.current_window_handle
                    driver.execute_script("window.open(arguments[0]);", href)
                    
                    # Switch back to original tab
                    driver.switch_to.window(original_window)
                    
                except Exception as e:
                    print(f"Failed to initiate download: {str(e)}")
                    continue
                
                # Wait for download to complete
                download_complete = False
                timeout = time.time() + SELENIUM_TIMEOUT
                
                while time.time() < timeout:
                    current_files = set(os.listdir(output_dir_path))
                    new_files = current_files - initial_files
                    
                    # Check for completed downloads
                    completed_files = [f for f in new_files if not f.endswith(('.crdownload', '.tmp'))]
                    
                    if completed_files:
                        downloaded_file = completed_files[0]
                        final_path = os.path.join(output_dir_path, filename)
                        
                        # Rename the file if it exists
                        if os.path.exists(final_path):
                            base, ext = os.path.splitext(filename)
                            counter = 1
                            while os.path.exists(final_path):
                                final_path = os.path.join(output_dir_path, f"{base}_{counter}{ext}")
                                counter += 1
                        
                        # Move the file to its final location
                        os.rename(os.path.join(output_dir_path, downloaded_file), final_path)
                        print(f"Saved as: {os.path.basename(final_path)}")
                        success_count += 1
                        download_complete = True
                        break
                    
                    time.sleep(0.5)
                
                if not download_complete:
                    print("Download timed out")
                    
            except Exception as e:
                print(f"Error downloading document: {str(e)}")
                continue
            
            # Add delay between downloads
            time.sleep(REQUEST_DELAY)
        
        print(f"\nDownloaded {success_count}/{len(valid_downloads)} documents")
        
        # Track incomplete downloads
        if success_count < len(valid_downloads):
            download_tracker.add_incomplete_download(
                url, cpv, doc_id,
                len(valid_downloads), success_count
            )
            
        # If no documents were downloaded, try to remove the empty directory
        if success_count == 0:
            try:
                os.rmdir(output_dir_path)
            except:
                pass
            return False
            
        return success_count > 0
        
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        failed_urls.append(url)
        return False

def process_csv(csv_path):
    """Process CSV file with Selenium WebDriver and resume functionality"""
    try:
        # Load previous progress
        processed_urls = download_tracker.load_progress()
        print(f"Loaded progress: {len(processed_urls)} URLs previously processed")
        
        # First pass: count valid URLs and validate CSV structure
        valid_entries = []
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Validate CSV structure
            if CSV_URL_FIELD not in reader.fieldnames:
                print(f"Error: CSV missing required column '{CSV_URL_FIELD}'")
                sys.exit(1)
                
            if 'cpvs' not in reader.fieldnames or 'id' not in reader.fieldnames:
                print("Error: CSV must contain 'cpvs' and 'id' columns")
                sys.exit(1)
            
            # Count valid entries
            total_urls = 0
            for row in reader:
                total_urls += 1
                url = row[CSV_URL_FIELD].strip()
                cpv = row['cpvs'].strip()
                doc_id = row['id'].strip()
                
                if url and cpv and doc_id and is_valid_url(url):
                    # Skip already processed URLs unless they had incomplete downloads
                    if url not in processed_urls:
                        valid_entries.append({
                            'url': url,
                            'cpv': cpv,
                            'id': doc_id
                        })

        # Show detailed summary
        total_valid = len(valid_entries)
        if total_valid == 0:
            if len(processed_urls) > 0:
                print("\nAll URLs have been processed!")
                download_tracker.print_summary()
                download_tracker.save_error_report()
                return
            else:
                print(f"\nNo valid URLs found that start with {BASE_DOMAIN}")
                sys.exit(1)

        print(f"\nURL Analysis:")
        print(f"Total URLs in CSV: {total_urls}")
        print(f"Previously processed: {len(processed_urls)}")
        print(f"Remaining to process: {total_valid}")
        
        # Ask for confirmation
        response = input("\nDo you want to proceed with the extraction? (yes/no): ").lower().strip()
        if response not in ('yes', 'y'):
            print("Extraction cancelled by user.")
            sys.exit(0)

        # Initialize WebDriver
        print("\nInitializing Chrome WebDriver...")
        driver = setup_selenium_driver()
        
        try:
            # Process URLs
            success = 0
            skipped = 0
            
            for i, entry in enumerate(valid_entries, 1):
                print(f"\nProcessing {i}/{total_valid}: {entry['url']}")
                
                if extract_documents(driver, entry['url'], entry['cpv'], entry['id']):
                    success += 1
                    processed_urls.add(entry['url'])
                else:
                    skipped += 1
                    
                # Save progress after each URL
                download_tracker.save_progress(processed_urls)
                
                # Add delay between URLs
                if i < total_valid:
                    time.sleep(REQUEST_DELAY)

            print(f"\nExtraction complete!")
            print(f"Successfully processed: {success}/{total_valid} entries")
            print(f"Skipped/Failed: {skipped} entries")
            
            # Print summary of incomplete downloads
            download_tracker.print_summary()
            
            # Save final error report
            download_tracker.save_error_report()
            
            if failed_urls:
                print(f"\nThe following URLs could not be accessed:")
                for url in failed_urls:
                    print(f"- {url}")
                    
        finally:
            print("\nClosing browser...")
            driver.quit()

    except Exception as e:
        print(f"Critical error: {str(e)}")
        print("Full error details:", str(sys.exc_info()))
        sys.exit(1)

def setup_selenium_driver(download_dir=None):
    """Set up and configure Chrome WebDriver with cookies and download settings"""
    chrome_options = Options()
    
    # Set up download preferences
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
        # Force download PDFs instead of opening them
        "plugins.always_open_pdf_externally": True,
        "download.default_directory": os.path.abspath(download_dir if download_dir else os.getcwd())
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Add necessary Chrome options
    chrome_options.add_argument("--headless=new")  # Use new headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Create and configure the driver
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    # Set timeouts
    driver.set_page_load_timeout(SELENIUM_TIMEOUT)
    driver.implicitly_wait(10)
    
    # Initialize cookies
    print("Setting up cookies...")
    driver.get(BASE_DOMAIN)
    
    for name, value in INITIAL_COOKIES.items():
        try:
            driver.add_cookie({
                'name': name,
                'value': value,
                'domain': '.vortal.biz'
            })
        except Exception as e:
            print(f"Warning: Could not set cookie {name}: {str(e)}")
            
    print("Cookies set successfully")
    return driver

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python vortal_extract.py <path_to_csv>")
        sys.exit(1)
        
    process_csv(sys.argv[1])