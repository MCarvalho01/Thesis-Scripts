import os
import json
import csv
import time
import re
import traceback
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from typing import Dict, List, Any

# Load environment variables
load_dotenv('.env.local')

# Configure OpenAI
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
MODEL_NAME = os.getenv('MODEL_NAME', 'gpt-4o')

# Constants
PROGRESS_FILE = 'analysis_progress.json'
MAX_TOKENS = 3500
INPUT_CSV = 'final_contracts.csv'

def load_contract_data():
    """Load contract data from final_contracts.csv maintaining order"""
    contract_data = []
    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                contract_id = row.get('id', '').strip()
                if contract_id:
                    contract_data.append({
                        'id': contract_id,
                        'contracting_main': row.get('contracting_main', '').strip()
                    })
    except Exception as e:
        print(f"Error loading contract data: {e}")
        print(traceback.format_exc())
    return contract_data

def create_analysis_prompt(text: str) -> str:
    """Create the analysis prompt for the OpenAI API."""
    return f"""Analyze this contract text to identify the pricing mechanism type and pricing components:

**Key Information to Extract**
1. Pricing Mechanism Type (MUST be one of these exact values):
   - "Fixed Price": If the contract specifies fixed prices for gas
   - "MIBGAS-based": If prices are based on MIBGAS index
   - "TTF-based": If prices are based on TTF index
   - "Mixed": If the contract uses a combination of mechanisms
   - "Unclear": If the pricing mechanism cannot be determined

2. Pricing Components:
   - Fixed price components (valor da parcela de preço fixo)
   - Variable price components (componentes variáveis)
   - Price per kWh (EUR/kWh)
   - Any additional fees or charges

3. Look for specific sections:
   - "As seguintes componentes de preço"
   - "Estrutura tarifária"
   - "Preço do gás natural"
   - "Condições económicas"
   - "Tarifas aplicáveis"

4. Additional Terms:
   - Payment conditions
   - Price adjustment mechanisms
   - Special pricing conditions

**Contract Text**
{text}

**JSON Output**
{{
  "gas_type": "MUST be one of: Fixed Price, MIBGAS-based, TTF-based, Mixed, Unclear",
  "pricing_components": {{
    "fixed_price": "Value in EUR if found",
    "variable_components": ["List of variable components found"],
    "price_per_kwh": "Value if found",
    "additional_fees": ["List of additional fees found"]
  }},
  "extracted_clauses": ["Relevant text excerpts about pricing"],
  "confidence_score": "0-100",
  "notes": "Any additional relevant information found"
}}"""

def split_text_into_chunks(text: str, max_size: int = MAX_TOKENS) -> List[str]:
    """Split text into chunks of maximum size while preserving sentences."""
    if len(text) <= max_size:
        return [text]
    
    chunks = []
    current_chunk = ""
    
    # Split by paragraphs first
    paragraphs = text.split('\n\n')
    
    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 2 <= max_size:  # +2 for '\n\n'
            current_chunk += (paragraph + '\n\n')
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = paragraph + '\n\n'
            
            # If a single paragraph is too large, split it by sentences
            while len(current_chunk) > max_size:
                split_point = current_chunk.rfind('. ', 0, max_size)
                if split_point == -1:
                    split_point = max_size
                chunks.append(current_chunk[:split_point].strip())
                current_chunk = current_chunk[split_point:].strip() + '\n\n'
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

def analyze_large_text(text: str) -> Dict[str, Any]:
    """Analyze large text by breaking it into chunks and combining results."""
    # Define valid gas types
    VALID_GAS_TYPES = {"Fixed Price", "MIBGAS-based", "TTF-based", "Mixed", "Unclear"}
    
    # Quick check for relevant sections
    quick_check = text.lower()
    has_pricing = any(term in quick_check for term in [
        'componentes de preço',
        'eur/kwh',
        'preço fixo',
        'estrutura tarifária',
        'condições económicas',
        'tarifas aplicáveis',
        'mibgas',
        'ttf'
    ])
    
    # Find relevant paragraphs
    relevant_paragraphs = []
    paragraphs = text.split('\n\n')
    keywords = [
        'preço', 'tarifa', 'eur', 'kwh', 'gás', 'componente',
        'fixo', 'variável', 'estrutura', 'económica', 'pagamento',
        'valor', 'parcela', 'mibgas', 'ttf', 'indexação'
    ]
    
    for para in paragraphs:
        para_lower = para.lower()
        if any(keyword in para_lower for keyword in keywords):
            relevant_paragraphs.append(para)
    
    if not has_pricing:
        return {
            "gas_type": "Unclear",
            "pricing_components": {
                "fixed_price": "",
                "variable_components": [],
                "price_per_kwh": "",
                "additional_fees": []
            },
            "extracted_clauses": [],
            "confidence_score": 0,
            "notes": "No relevant pricing information found"
        }
    
    # Process relevant paragraphs
    if relevant_paragraphs:
        text_to_analyze = '\n\n'.join(relevant_paragraphs)
        chunks = split_text_into_chunks(text_to_analyze)
    else:
        chunks = split_text_into_chunks(text)
    
    # Analyze each chunk
    all_results = []
    for i, chunk in enumerate(chunks):
        print(f"Analyzing chunk {i+1}/{len(chunks)}")
        result = call_openai_api(chunk)
        
        if result:
            # Ensure confidence_score is a float
            try:
                result['confidence_score'] = float(result.get('confidence_score', 0))
            except (ValueError, TypeError):
                result['confidence_score'] = 0.0
            
            # Validate gas type
            gas_type = result.get('gas_type', 'Unclear')
            if gas_type not in VALID_GAS_TYPES:
                print(f"Warning: Invalid gas type '{gas_type}', defaulting to 'Unclear'")
                result['gas_type'] = 'Unclear'
                result['confidence_score'] = max(result['confidence_score'] * 0.5, 0)  # Reduce confidence for invalid type
            
            if result['confidence_score'] > 0:
                all_results.append(result)
        
        time.sleep(0.2)  # Small delay between chunks
    
    if not all_results:
        return {
            "gas_type": "Unclear",
            "pricing_components": {
                "fixed_price": "",
                "variable_components": [],
                "price_per_kwh": "",
                "additional_fees": []
            },
            "extracted_clauses": [],
            "confidence_score": 0,
            "notes": "No clear information found in any chunk"
        }
    
    # Combine results from all chunks
    # Ensure all confidence scores are float for comparison
    best_result = max(all_results, key=lambda x: float(x.get('confidence_score', 0)))
    
    # Merge extracted clauses from all results
    all_clauses = []
    for result in all_results:
        if result.get('extracted_clauses'):
            all_clauses.extend(result['extracted_clauses'])
    
    best_result['extracted_clauses'] = list(set(all_clauses))  # Remove duplicates
    
    # Final validation of gas type
    if best_result['gas_type'] not in VALID_GAS_TYPES:
        best_result['gas_type'] = 'Unclear'
    
    # Ensure the final confidence score is a string for consistency in output
    best_result['confidence_score'] = str(best_result['confidence_score'])
    
    return best_result

def call_openai_api(text: str) -> Dict[str, Any]:
    """Call OpenAI API with retry logic and error handling."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a contract analyst specializing in gas contracts. Extract gas type and pricing information. Respond with valid JSON only."
                    },
                    {
                        "role": "user",
                        "content": create_analysis_prompt(text)
                    }
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            try:
                content = response.choices[0].message.content
                return json.loads(content)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {str(e)}")
                if attempt == max_retries - 1:
                    return {
                        "gas_type": "Error",
                        "pricing_components": {},
                        "extracted_clauses": [],
                        "confidence_score": 0,
                        "notes": "Failed to parse response"
                    }
            
        except Exception as e:
            if attempt == max_retries - 1:
                return {
                    "gas_type": "Error",
                    "pricing_components": {},
                    "extracted_clauses": [],
                    "confidence_score": 0,
                    "notes": f"API Error: {str(e)}"
                }
            time.sleep(2 ** attempt)

def get_contract_folders(contract_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Get contract folders maintaining order from final_contracts.csv"""
    base_path = os.path.join(os.getcwd(), 'Extracted_txt')
    contracts = []
    folder_map = {}
    
    if not os.path.exists(base_path):
        print(f"Directory {base_path} does not exist!")
        return contracts
    
    print(f"Scanning directory: {base_path}")
    folders = os.listdir(base_path)
    print(f"Found {len(folders)} folders")
    
    # First, create a map of contract IDs to folder information
    for folder in folders:
        folder_path = os.path.join(base_path, folder)
        if os.path.isdir(folder_path):
            parts = folder.split('_')
            if len(parts) > 1:
                contract_id = parts[-1]  # Get the last part after the last underscore
                if contract_id.isdigit():
                    folder_map[contract_id] = {
                        'folder_name': folder,
                        'folder_path': folder_path
                    }
    
    # Then process contracts in the order they appear in final_contracts.csv
    for contract in contract_data:
        contract_id = contract['id']
        folder_info = folder_map.get(contract_id)
        
        if folder_info:
            contracts.append({
                'id': contract_id,
                'contracting_main': contract['contracting_main'],
                'folder_name': folder_info['folder_name'],
                'folder_path': folder_info['folder_path']
            })
            print(f"Found contract ID: {contract_id} in folder: {folder_info['folder_name']}")
        else:
            print(f"Warning: No folder found for contract ID: {contract_id}")
    
    print(f"\nTotal contracts found: {len(contracts)}")
    return contracts

def read_contract_files(folder_path: str) -> List[str]:
    """Read all text files in a contract folder."""
    texts = []
    
    try:
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if content.strip():  # Only add non-empty content
                                texts.append(content)
                                print(f"Read file: {file} ({len(content)} chars)")
                    except Exception as e:
                        print(f"Error reading file {file}: {str(e)}")
                        print(traceback.format_exc())
    except Exception as e:
        print(f"Error reading folder {folder_path}: {str(e)}")
        print(traceback.format_exc())
    
    return texts

def analyze_contracts(output_file: str = "analyzed_gas_contracts.csv", resume: bool = True):
    """Main function to analyze contracts from Extracted_txt directory."""
    try:
        # Load contract data from CSV first
        contract_data = load_contract_data()
        if not contract_data:
            raise ValueError("No contracts found in final_contracts.csv")
        
        # Load progress if resuming
        progress = load_progress() if resume else {
            'completed_ids': [],
            'last_processed_index': -1,
            'total_contracts': 0,
            'start_time': datetime.now().isoformat(),
            'completed_contracts': 0,
            'errors': []
        }
        
        # Get all contract folders in the same order as final_contracts.csv
        contracts = get_contract_folders(contract_data)
        if not contracts:
            raise ValueError("No contract folders found in Extracted_txt directory")
        
        # Update total contracts in progress
        progress['total_contracts'] = len(contracts)
        
        # Load existing output if resuming
        output_rows = []
        if resume and os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    output_reader = csv.DictReader(f)
                    output_rows = list(output_reader)
                print(f"Loaded {len(output_rows)} previously analyzed contracts")
            except Exception as e:
                print(f"Error loading existing output: {e}")
        
        print(f"Processing {len(contracts)} contracts...")
        print(f"Resuming from contract {progress['last_processed_index'] + 1}" if resume else "Starting fresh")
        
        # Process each contract
        for i, contract in enumerate(contracts):
            # Skip already processed contracts if resuming
            if resume and i <= progress['last_processed_index']:
                continue
                
            if contract['id'] in progress['completed_ids']:
                continue
            
            try:
                print(f"\nProcessing contract {i+1}/{len(contracts)}")
                print(f"Contract ID: {contract['id']}")
                print(f"Contracting Entity: {contract['contracting_main']}")
                
                # Read all text files for this contract
                texts = read_contract_files(contract['folder_path'])
                
                if not texts:
                    print(f"No text files found for contract {contract['id']}")
                    continue
                
                print(f"Found {len(texts)} text files")
                
                # Create new row with basic info
                output_row = {
                    'id': contract['id'],
                    'contracting_main': contract['contracting_main']
                }
                
                # Combine all texts for analysis
                contract_text = "\n\n".join(texts)
                print(f"Total combined text length: {len(contract_text)}")
                
                print("Starting analysis...")
                analysis_result = analyze_large_text(contract_text)
                print(f"Analysis complete. Result: {analysis_result['gas_type']}")
                
                # Add analysis results to the row
                output_row['gas_type'] = analysis_result['gas_type']
                output_row['fixed_price'] = analysis_result['pricing_components'].get('fixed_price', '')
                output_row['variable_components'] = ', '.join(analysis_result['pricing_components'].get('variable_components', []))
                output_row['price_per_kwh'] = analysis_result['pricing_components'].get('price_per_kwh', '')
                output_row['additional_fees'] = ', '.join(analysis_result['pricing_components'].get('additional_fees', []))
                output_row['extracted_clauses'] = ', '.join(analysis_result['extracted_clauses'])
                output_row['confidence_score'] = analysis_result['confidence_score']
                output_row['notes'] = analysis_result['notes']
                
                output_rows.append(output_row)
                
                # Update progress
                progress['completed_ids'].append(contract['id'])
                progress['last_processed_index'] = i
                progress['completed_contracts'] += 1
                save_progress(progress)
                
                # Write current results to CSV
                if output_rows:
                    write_output_csv(output_rows, output_file)
                    print(f"Saved results for contract {contract['id']}")
                
            except Exception as e:
                error_msg = f"Error processing contract {contract['id']}: {str(e)}"
                print(error_msg)
                print(f"Full error: {traceback.format_exc()}")
                progress['errors'].append({
                    'id': contract['id'],
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
                save_progress(progress)
            
            time.sleep(0.2)  # Small delay between contracts
        
        print(f"\nAnalysis complete! Results written to {output_file}")
        print(f"Processed {len(output_rows)} contracts")
        
        if progress['errors']:
            print("\nErrors encountered:")
            for error in progress['errors']:
                print(f"Contract {error['id']}: {error['error']}")
        
        return True
        
    except Exception as e:
        print(f"Error processing contracts: {str(e)}")
        print(f"Full error: {traceback.format_exc()}")
        raise

def write_output_csv(output_rows: List[Dict[str, Any]], output_file: str):
    """Write output rows to CSV file."""
    if not output_rows:
        return
        
    # Define the essential columns for output
    fieldnames = [
        'id',
        'contracting_main',
        'gas_type',
        'fixed_price',
        'variable_components',
        'price_per_kwh',
        'additional_fees',
        'extracted_clauses',
        'confidence_score',
        'notes'
    ]
    
    # Create clean output rows with only essential columns
    clean_output_rows = []
    for row in output_rows:
        # Ensure all fields exist with proper types
        clean_row = {
            'id': str(row.get('id', '')),
            'contracting_main': str(row.get('contracting_main', '')),
            'gas_type': str(row.get('gas_type', 'Not specified')),
            'fixed_price': str(row.get('fixed_price', '')),
            'variable_components': str(row.get('variable_components', '')),
            'price_per_kwh': str(row.get('price_per_kwh', '')),
            'additional_fees': str(row.get('additional_fees', '')),
            'extracted_clauses': str(row.get('extracted_clauses', '')),
            'confidence_score': str(row.get('confidence_score', '0')),
            'notes': str(row.get('notes', ''))
        }
        clean_output_rows.append(clean_row)
    
    # Write to temporary file first
    temp_file = output_file + '.tmp'
    with open(temp_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_output_rows)
    
    # Then rename to final file
    try:
        os.replace(temp_file, output_file)
    except Exception as e:
        print(f"Error saving to final file: {e}")
        # If rename fails, try direct write
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(clean_output_rows)

def load_progress() -> Dict[str, Any]:
    """Load progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading progress file: {e}")
    return {
        'completed_ids': [],
        'last_processed_index': -1,
        'total_contracts': 0,
        'start_time': datetime.now().isoformat(),
        'completed_contracts': 0,
        'errors': []
    }

def save_progress(progress: Dict[str, Any]):
    """Save progress to JSON file."""
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        print(f"Error saving progress: {e}")

if __name__ == "__main__":
    if not os.getenv('OPENAI_API_KEY'):
        print("Error: OPENAI_API_KEY not found in .env.local")
        print("Please add your OpenAI API key to the .env.local file")
        exit(1)
    
    # Add command line argument parsing
    import argparse
    parser = argparse.ArgumentParser(description='Analyze gas contracts using OpenAI API')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh analysis without resuming')
    parser.add_argument('--output', default='analyzed_gas_contracts.csv', help='Output CSV file path')
    args = parser.parse_args()
    
    analyze_contracts(
        output_file=args.output,
        resume=not args.no_resume
    )