import csv
import requests
import os
import sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Constants
BASE_DOMAIN = "https://plataforma-sncp.espap.gov.pt"
CSV_URL_FIELD = 'contractingProcedureUrl'

def is_valid_url(url):
    """Check if URL starts with the required domain"""
    return url.startswith(BASE_DOMAIN)

def extract_documents(session, url, cpv, doc_id):
    """Extract all documents from a portal page"""
    try:
        # Get the portal page
        response = session.get(url)
        response.raise_for_status()
        
        # Parse document links
        soup = BeautifulSoup(response.text, 'html.parser')
        documents = []
        
        # Find all download links
        for link in soup.select('a[href^="downloadDiretoDocumento"]'):
            # Get display filename from parent TD
            display_name = link.find_previous(text=True).strip()
            if not display_name:
                continue
                
            # Get download URL
            relative_url = link['href']
            download_url = urljoin(BASE_DOMAIN, relative_url)
            
            documents.append({
                'name': display_name,
                'url': download_url
            })
        
        if not documents:
            print("No documents found on page - skipping folder creation")
            return False
            
        # Only create directory if documents were found
        output_dir = f"{sanitize_filename(cpv)}_{sanitize_filename(doc_id)}"
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created folder: {output_dir}")
            
        # Download all found documents
        success_count = 0
        for doc in documents:
            try:
                file_response = session.get(doc['url'], stream=True)
                file_response.raise_for_status()
                
                # Clean filename
                clean_name = sanitize_filename(doc['name'])
                filename = os.path.join(output_dir, clean_name)
                filename = get_unique_filename(filename)
                
                with open(filename, 'wb') as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            
                print(f"Saved: {filename}")
                success_count += 1
                
            except Exception as e:
                print(f"Failed to download {doc['name']}: {str(e)}")
        
        print(f"Downloaded {success_count}/{len(documents)} documents")
        return success_count > 0

    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return False

def process_csv(csv_path):
    """Process CSV file with enhanced error handling"""
    try:
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
            for row in reader:
                url = row[CSV_URL_FIELD].strip()
                cpv = row['cpvs'].strip()
                doc_id = row['id'].strip()
                
                if url and cpv and doc_id and is_valid_url(url):
                    valid_entries.append({
                        'url': url,
                        'cpv': cpv,
                        'id': doc_id
                    })

        # Show summary and ask for confirmation
        total_extractions = len(valid_entries)
        if total_extractions == 0:
            print(f"\nNo valid URLs found that start with {BASE_DOMAIN}")
            sys.exit(1)

        print(f"\nFound {total_extractions} valid entries to process.")
        print(f"Folders will be created only for entries that contain documents")
        print("\nExample potential folders (will only be created if documents exist):")
        for entry in valid_entries[:3]:  # Show first 3 examples
            folder_name = f"{sanitize_filename(entry['cpv'])}_{sanitize_filename(entry['id'])}"
            print(f"- {folder_name}")
        if total_extractions > 3:
            print(f"- ...and {total_extractions - 3} more potential folders")

        # Ask for confirmation
        response = input("\nDo you want to proceed with the extraction? (yes/no): ").lower().strip()
        if response not in ('yes', 'y'):
            print("Extraction cancelled by user.")
            sys.exit(0)

        # Proceed with extraction
        print("\nStarting extraction...")
        with requests.Session() as session:
            # Configure session
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Language': 'pt-PT,pt;q=0.9,en;q=0.8',
            })

            success = 0
            skipped = 0
            for i, entry in enumerate(valid_entries, 1):
                print(f"\nProcessing {i}/{total_extractions}: {entry['url']}")
                if extract_documents(session, entry['url'], entry['cpv'], entry['id']):
                    success += 1
                else:
                    skipped += 1

            print(f"\nExtraction complete!")
            print(f"Successfully processed: {success}/{total_extractions} entries")
            print(f"Skipped (no documents): {skipped} entries")

    except Exception as e:
        print(f"Critical error: {str(e)}")
        sys.exit(1)

def sanitize_filename(filename):
    """Clean unsafe characters from filenames"""
    keep_chars = (' ', '.', '_', '-', '(')
    return "".join(c for c in filename if c.isalnum() or c in keep_chars).rstrip()

def get_unique_filename(filename):
    """Prevent file overwrites"""
    base, ext = os.path.splitext(filename)
    counter = 0
    while os.path.exists(filename):
        counter += 1
        filename = f"{base}_{counter}{ext}"
    return filename

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_documents.py <path_to_csv>")
        sys.exit(1)
        
    process_csv(sys.argv[1])