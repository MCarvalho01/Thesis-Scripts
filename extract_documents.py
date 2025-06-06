import csv
import requests
import os
import sys
from urllib.parse import urlparse
import mimetypes

# Constants for URL filtering
BASE_URLS = [
    "https://www.acingov.pt/"
]

def is_valid_url(url):
    """Check if URL matches any of the valid patterns"""
    return any(url.startswith(base_url) for base_url in BASE_URLS)

def extract_documents(url, output_name):
    """
    Improved version with better error handling and file type detection
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Always use .zip extension
        filename = f"{output_name}.zip"
        
        # Create unique filename if it already exists
        filename = get_unique_filename(filename)
        
        with open(filename, 'wb') as f:
            f.write(response.content)
            
        print(f"Successfully saved: {filename}")
        return True

    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        return False

def get_unique_filename(filename):
    """Prevent file overwrites"""
    base, ext = os.path.splitext(filename)
    counter = 0
    while os.path.exists(filename):
        counter += 1
        filename = f"{base}_{counter}{ext}"
    return filename

def sanitize_filename(filename):
    """Clean unsafe characters from filenames"""
    keep_chars = (' ', '.', '_', '-')
    return "".join(c for c in filename if c.isalnum() or c in keep_chars).rstrip()

def process_csv(csv_path):
    """Process all URLs in CSV file that match any of the valid URL patterns"""
    try:
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found: {csv_path}")
            sys.exit(1)
            
        if os.path.getsize(csv_path) == 0:
            print("Error: CSV file is empty")
            sys.exit(1)

        # First count and verify matching URLs
        print("\nChecking for URLs matching any of these patterns:")
        for base_url in BASE_URLS:
            print(f"- {base_url}")
            
        matching_urls = []
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Print available columns for debugging
            print("\nAvailable columns in CSV:")
            if reader.fieldnames:
                for i, header in enumerate(reader.fieldnames):
                    print(f"{i}: {header}")
            
            if 'contractingProcedureUrl' not in reader.fieldnames:
                print("Error: CSV file does not contain 'contractingProcedureUrl' column")
                print(f"Available columns are: {', '.join(reader.fieldnames)}")
                sys.exit(1)
                
            if 'cpvs' not in reader.fieldnames:
                print("Error: CSV file does not contain 'cpvs' column")
                print(f"Available columns are: {', '.join(reader.fieldnames)}")
                sys.exit(1)
                
            if 'id' not in reader.fieldnames:
                print("Error: CSV file does not contain 'id' column")
                print(f"Available columns are: {', '.join(reader.fieldnames)}")
                sys.exit(1)
            
            for row in reader:
                url = row.get('contractingProcedureUrl', '').strip()
                if url and is_valid_url(url):
                    matching_urls.append((url, row))

        num_files = len(matching_urls)
        if num_files == 0:
            print("\nNo URLs found matching any of the required patterns")
            sys.exit(1)

        print(f"\nFound {num_files} documents to download from matching URLs:")
        for url, _ in matching_urls[:5]:  # Show first 5 as examples
            print(f"- {url}")
        if num_files > 5:
            print(f"- ...({num_files - 5} more URLs)...")

        # Ask for confirmation
        response = input("\nDo you want to continue with the extraction? (yes/no): ").lower().strip()
        if response not in ('yes', 'y'):
            print("Extraction cancelled by user.")
            sys.exit(0)

        print("\nStarting extraction...")
        
        # Process the matched URLs
        successful = 0
        failed = 0
        for url, row in matching_urls:
            # Get CPV and ID values
            cpv = row.get('cpvs', '').strip()
            if not cpv:
                print(f"Warning: Missing CPV value for URL {url}, skipping...")
                failed += 1
                continue
                
            doc_id = row.get('id', '').strip()
            if not doc_id:
                print(f"Warning: Missing ID value for URL {url}, skipping...")
                failed += 1
                continue
            
            # Create filename pattern: cpv_id (sanitized)
            output_base = f"{sanitize_filename(cpv)}_{sanitize_filename(doc_id)}"
            
            print(f"\nDownloading from {url}")
            print(f"Will save as: {output_base}.zip")
            if extract_documents(url, output_base):
                successful += 1
            else:
                failed += 1

        # Print summary
        print(f"\nExtraction complete!")
        print(f"Successfully downloaded: {successful} files")
        print(f"Failed downloads: {failed} files")

    except csv.Error as e:
        print(f"CSV Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        print("Exception details:", str(sys.exc_info()))
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_documents.py <path_to_csv>")
        sys.exit(1)
        
    process_csv(sys.argv[1])