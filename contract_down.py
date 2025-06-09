import os
import csv
import json
import requests
import time
from urllib.parse import unquote

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.base.gov.pt/Base4/pt/pesquisa/',
    'DNT': '1'
}

def sanitize_filename(filename):
    """Remove invalid characters from filenames"""
    keep_chars = (' ', '.', '_', '-')
    return "".join(c if c.isalnum() or c in keep_chars else '_' for c in filename).strip()

def download_contract(session, document_id, document_name, folder_path, cpv, contract_id):
    try:
        pdf_url = f"https://www.base.gov.pt/Base4/pt/resultados/?type=doc_documentos&id={document_id}&ext=.pdf"
        response = session.get(pdf_url, timeout=15)
        
        if response.status_code == 200:
            filename = f"{cpv}_{contract_id}.pdf"
            file_path = os.path.join(folder_path, filename)
            
            counter = 1
            while os.path.exists(file_path):
                file_path = os.path.join(folder_path, f"{cpv}_{contract_id}_{counter}.pdf")
                counter += 1
                
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Saved: {filename}")
            return True
                
        print(f"Failed to download {document_name} (HTTP {response.status_code})")
        return False
        
    except Exception as e:
        print(f"Error downloading {document_name}: {str(e)}")
        return False

def process_csv(csv_path, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    
    with requests.Session() as session:
        session.headers.update(HEADERS)
        session.get("https://www.base.gov.pt/Base4/pt/pesquisa/", timeout=10)
        
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_number, row in enumerate(reader, start=1):
                try:
                    # Check if URL matches the required pattern
                    procedure_url = row.get('contractingProcedureUrl', '').strip()
                    if not procedure_url.startswith("https://plataforma-sncp.espap.gov.pt/"):
                        print(f"Row {row_number}: Skipped - URL pattern not matched")
                        continue
                    
                    cpv = row.get('cpvs', '').strip()
                    contract_id = row.get('id', '').strip()
                    
                    if not cpv or not contract_id:
                        print(f"Row {row_number}: Missing cpv or id")
                        continue
                    
                    documents = json.loads(row['documents'].replace("'", '"'))
                    
                    for doc in documents:
                        document_id = doc['id']
                        document_name = unquote(doc['description'])
                        
                        if download_contract(session, document_id, document_name, output_folder, cpv, contract_id):
                            time.sleep(1)
                            
                except json.JSONDecodeError:
                    print(f"Row {row_number}: Invalid documents JSON")
                except KeyError as e:
                    print(f"Row {row_number}: Missing key {str(e)}")
                except Exception as e:
                    print(f"Row {row_number} error: {str(e)}")
                    continue

if __name__ == "__main__":
    CSV_FILE = "final_contracts.csv"
    OUTPUT_FOLDER = "downloaded_contracts"
    
    print(f"Starting download to: {os.path.abspath(OUTPUT_FOLDER)}")
    process_csv(CSV_FILE, OUTPUT_FOLDER)
    print("Processing complete! Check the output folder.")