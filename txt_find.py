import pandas as pd
import csv
import io
import os
import glob

def clean_csv_content(file_path):
    """Read and clean the CSV content before parsing."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Print the first line of raw content for debugging
    print("\nFirst row of raw input file:")
    first_line = content.split('\n')[0]
    print(first_line)
    
    # Remove the description;;;;;;; part from the header
    content = content.replace('description;;;;;;;', 'description')
    
    # Split into lines
    lines = content.split('\n')
    
    # Clean the header (first line)
    header = lines[0].strip('"').split('","')
    header = [col.strip() for col in header]
    
    # Print cleaned header for verification
    print("\nCleaned header:")
    print(header)
    
    # Clean the data rows
    cleaned_lines = []
    cleaned_lines.append(','.join(header))
    
    # Print first data row for debugging
    if len(lines) > 1:
        print("\nFirst data row (raw):")
        print(lines[1])
    
    for line in lines[1:]:
        if not line.strip():  # Skip empty lines
            continue
        # Split the line by commas, but only outside of quotes
        parts = []
        current = []
        in_quotes = False
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        parts.append(''.join(current))
        
        # Clean each part
        cleaned_parts = []
        for part in parts:
            part = part.strip().strip('"').strip()
            if ',' in part:  # If part contains commas, wrap in quotes
                part = f'"{part}"'
            cleaned_parts.append(part)
        
        cleaned_lines.append(','.join(cleaned_parts))
        
        # Print first cleaned data row for debugging
        if line == lines[1]:
            print("\nFirst data row (cleaned):")
            print(cleaned_lines[-1])
    
    return '\n'.join(cleaned_lines)

def extract_csv_fields(input_file, output_file, selected_fields):
    """
    Extract specific fields from input CSV and save them to a new CSV file.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path where the output CSV will be saved
        selected_fields (list): List of column names to extract
    """
    try:
        # Clean the CSV content first
        cleaned_content = clean_csv_content(input_file)
        
        # Read the cleaned CSV content with pandas
        df = pd.read_csv(io.StringIO(cleaned_content))
        
        # Print available columns for debugging
        print("\nAvailable columns in CSV:")
        for col in df.columns:
            print(f"- '{col}'")
        
        # Select only the specified fields
        df_selected = df[selected_fields]
        
        # Print sample of data for verification
        print("\nFirst few rows of selected data:")
        print(df_selected.head())
        
        # Save to new CSV with the desired format
        df_selected.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"\nSuccessfully created {output_file} with selected fields")
        print(f"Number of rows extracted: {len(df_selected)}")
        
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def clean_header(header):
    """Remove trailing special characters and whitespace"""
    return header.split(';')[0].strip()

def extract_precise_data(input_file, output_file, fields):
    """Extract specific fields from CSV with proper formatting handling"""
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            # Read and clean headers
            reader = csv.reader(f, delimiter=',', quotechar='"', escapechar='\\')
            
            # Clean headers by removing trailing semicolons
            headers = [col.split(';')[0].strip() for col in next(reader)]
            print("Cleaned Headers:", headers)
            
            # Create case-insensitive column map
            column_map = {col.lower().strip(): idx for idx, col in enumerate(headers)}
            field_indices = []
            
            for field in fields:
                clean_field = field.lower().strip()
                if clean_field in column_map:
                    field_indices.append(column_map[clean_field])
                else:
                    raise ValueError(f"Column '{field}' not found in headers")

            # Read and validate data row
            data_row = next(reader)
            print(f"\nRaw data ({len(data_row)} columns): {data_row[:len(fields)+2]}")

            # Extract values with proper formatting
            extracted = []
            for idx in field_indices:
                try:
                    value = data_row[idx].strip()
                    # Preserve existing quotes or add new ones if needed
                    if ',' in value and not value.startswith('"'):
                        value = f'"{value}"'
                    extracted.append(value)
                except (IndexError, AttributeError):
                    extracted.append('')
                    print(f"Warning: Missing value for index {idx}")

            # Write output with original formatting
            with open(output_file, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(fields)
                writer.writerow(extracted)

            print("\nExtraction Results:")
            for name, value in zip(fields, extracted):
                print(f"{name}: {value}")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())

def parse_csv_line(line):
    """Parse a single CSV line with proper quote handling."""
    values = []
    current = []
    in_quotes = False
    i = 0
    while i < len(line):
        char = line[i]
        if char == '"':
            # Check for escaped quotes
            if i + 1 < len(line) and line[i + 1] == '"':
                current.append('"')
                i += 2
                continue
            in_quotes = not in_quotes
            i += 1
        elif char == ',' and not in_quotes:
            values.append(''.join(current))
            current = []
            i += 1
        else:
            current.append(char)
            i += 1
    if current:
        values.append(''.join(current))
    return values

def is_valid_row(values, field_indices):
    """Check if a row is a valid data row (not a continuation)."""
    if not values:
        return False
        
    # Get indices for key fields
    id_idx = field_indices[0]  # id
    contracting_idx = field_indices[1]  # contracting_main
    
    # Check if we have all required indices
    if any(idx >= len(values) for idx in [id_idx, contracting_idx]):
        return False
    
    # Get values for key fields
    id_val = values[id_idx].strip().lower()
    contracting_val = values[contracting_idx].strip().lower()
    
    # Check for invalid patterns that indicate continuation rows
    invalid_patterns = [
        'description', 'id":', 'nif":', '"}]', '"id":', 
        'false', 'true', 'nan', '[{', '}]', '"":'
    ]
    
    # Check if any field contains invalid patterns
    for pattern in invalid_patterns:
        if any(pattern in val for val in [id_val, contracting_val]):
            return False
    
    # Check that key fields have valid values
    return (
        # ID should be numeric
        id_val and id_val.isdigit() and
        # Contracting entity should be non-empty and not contain JSON-like content
        contracting_val and '{' not in contracting_val
    )

def get_document_files(id_value):
    """Get list of document files from the corresponding folder."""
    base_path = os.path.join(os.getcwd(), 'Extracted_txt')  # Get absolute path
    print(f"\nLooking for ID: {id_value}")
    print(f"Base path: {base_path}")
    
    # List all folders in the Extracted_txt directory
    if not os.path.exists(base_path):
        print(f"Directory {base_path} does not exist!")
        return None
        
    folders = os.listdir(base_path)
    print(f"Found {len(folders)} folders in {base_path}")
    print("First few folders:", folders[:5])  # Print first 5 folders for debugging
    
    # Find folder that ends with _id_value
    matching_folder = None
    for folder in folders:
        if folder.endswith(f"_{id_value}"):
            matching_folder = folder
            print(f"Found matching folder: {folder}")
            break
    
    if matching_folder is None:
        print(f"No folder found ending with _{id_value}")
        return None
        
    folder_path = os.path.join(base_path, matching_folder)
    print(f"Full folder path: {folder_path}")
    
    # Get all .txt files recursively in the folder and its subfolders
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        print(f"Searching in subfolder: {root}")
        for file in files:
            if file.endswith('.txt'):
                file_path = os.path.join(root, file)
                all_files.append(file_path)
                print(f"Found text file: {file}")
    
    print(f"Found {len(all_files)} text files in total")
    if all_files:
        # Get relative paths from the base folder
        relative_paths = [os.path.relpath(f, folder_path) for f in all_files]
        print("Files found (relative paths):", relative_paths)
        return relative_paths
    
    return []

def extract_raw_csv_data(input_file, output_file):
    """
    Extract specific fields from CSV while preserving the exact raw data format.
    Filters out continuation rows by checking for valid values in key fields.
    """
    try:
        # First check if input file exists and is not empty
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file {input_file} does not exist")
            
        file_size = os.path.getsize(input_file)
        if file_size == 0:
            raise ValueError(f"Input file {input_file} is empty")
            
        print(f"\nReading file: {input_file}")
        print(f"File size: {file_size} bytes")

        # Define the fields we want to extract from input CSV
        fields_to_extract = [
            'id',
            'contracting_main'
        ]

        # Track the maximum number of documents found
        max_documents = 0

        # First pass to determine how many document columns we need
        print("\nScanning folders to determine maximum number of documents...")
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            # Skip header
            next(f)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('"') and line.endswith('"'):
                    line = line[1:-1]
                values = parse_csv_line(line)
                if len(values) > 0:
                    id_value = values[0].strip().strip('"')
                    if id_value.isdigit():
                        document_files = get_document_files(id_value)
                        if document_files is not None and len(document_files) > max_documents:
                            max_documents = len(document_files)
                            print(f"Found folder with {max_documents} documents")

        # Ensure we have at least 8 document columns, but allow for more if needed
        max_documents = max(8, max_documents)
        
        # Create document columns based on maximum found
        document_columns = [f'Documento {i+1}' for i in range(max_documents)]
        print(f"\nCreated {len(document_columns)} document columns")
        print("Document columns:", document_columns)

        # All columns that will be in the output
        output_columns = fields_to_extract + document_columns

        # Create output file for invalid rows
        invalid_output = output_file.replace('.csv', '_invalid.csv')

        # Read and process the file
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            # Read header line
            header_line = f.readline().strip()
            if header_line.startswith('"') and header_line.endswith('"'):
                header_line = header_line[1:-1]
            headers = [h.split(';')[0].strip() for h in parse_csv_line(header_line)]
            
            # Create header map
            header_map = {h.lower(): i for i, h in enumerate(headers)}
            
            # Get field indices only for fields we want to extract
            field_indices = []
            for field in fields_to_extract:
                if field.lower() in header_map:
                    field_indices.append(header_map[field.lower()])
                else:
                    raise ValueError(f"Column '{field}' not found in headers")
            
            print("\nProcessing rows...")
            
            # Open both output files and write headers
            with open(output_file, 'w', encoding='utf-8', newline='') as out_f, \
                 open(invalid_output, 'w', encoding='utf-8', newline='') as invalid_f:
                writer = csv.writer(out_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                invalid_writer = csv.writer(invalid_f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                # Write headers including the new document columns
                writer.writerow(output_columns)
                invalid_writer.writerow(output_columns + ['reason_invalid'])
                
                # Process each data row
                row_count = 0
                skipped_count = 0
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:  # Skip empty lines
                        continue
                        
                    # Remove outer quotes if present
                    if line.startswith('"') and line.endswith('"'):
                        line = line[1:-1]
                    
                    # Parse the line
                    values = parse_csv_line(line)
                    
                    # Extract values for all fields
                    extracted_values = []
                    for idx in field_indices:
                        if idx < len(values):
                            value = values[idx].strip()
                            # Clean up nested quotes
                            while '""' in value:
                                value = value.replace('""', '"')
                            # Remove surrounding quotes if present
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            # Add quotes if needed
                            if ',' in value:
                                value = f'"{value}"'
                            extracted_values.append(value)
                        else:
                            extracted_values.append('')
                    
                    # Check if this is a valid row
                    if is_valid_row(values, field_indices):
                        # Get the ID value
                        id_value = extracted_values[0]
                        
                        # Check if folder exists and get document files
                        document_files = get_document_files(id_value)
                        
                        if document_files is not None:
                            # Add document filenames to the row
                            for i in range(len(document_columns)):
                                if i < len(document_files):
                                    extracted_values.append(document_files[i])
                                else:
                                    extracted_values.append('')
                                    
                            writer.writerow(extracted_values)
                            row_count += 1
                            
                            # Print progress every 100 rows
                            if row_count % 100 == 0:
                                print(f"Processed {row_count} valid rows...")
                            
                            # Print first row for verification
                            if row_count == 1:
                                print("\nFirst valid row values:")
                                for field, value in zip(output_columns, extracted_values):
                                    print(f"{field}: {value}")
                        else:
                            # No matching folder found, add to invalid rows
                            reason = "No matching document folder found"
                            extracted_values.extend([''] * len(document_columns))  # Add empty document columns
                            invalid_writer.writerow(extracted_values + [reason])
                            skipped_count += 1
                    else:
                        # For invalid rows, add the reason why it was considered invalid
                        reason = "Continuation row or invalid format"
                        if not values:
                            reason = "Empty row"
                        elif any(idx >= len(values) for idx in [field_indices[0], field_indices[1]]):
                            reason = "Missing required fields"
                        elif not values[field_indices[0]].strip():
                            reason = "Empty ID"
                        elif not values[field_indices[0]].strip().isdigit():
                            reason = "Non-numeric ID"
                        elif any(pattern in values[field_indices[0]].strip().lower() for pattern in ['description', 'id":', 'nif":', '"}]', '"id":']):
                            reason = "Contains JSON-like content"
                        
                        extracted_values.extend([''] * len(document_columns))  # Add empty document columns
                        invalid_writer.writerow(extracted_values + [reason])
                        skipped_count += 1
            
            print(f"\nTotal valid rows processed: {row_count}")
            print(f"Rows skipped (continuations/invalid): {skipped_count}")
            print(f"\nInvalid rows have been saved to: {invalid_output}")
        
        # Verify output
        print("\nVerifying output file...")
        with open(output_file, 'r', encoding='utf-8') as f:
            print("\nFirst few lines of output:")
            for i, line in enumerate(f):
                if i < 5:  # Print first 5 lines
                    print(f"Line {i+1}: {line.strip()}")
                else:
                    break
        
        # Show sample of invalid rows
        print("\nSample of invalid rows:")
        with open(invalid_output, 'r', encoding='utf-8') as f:
            print("\nFirst few lines of invalid rows:")
            for i, line in enumerate(f):
                if i < 5:  # Print first 5 lines
                    print(f"Line {i+1}: {line.strip()}")
                else:
                    break
        
        return True
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

# Usage
if __name__ == "__main__":
    success = extract_raw_csv_data(
        input_file="final_contracts.csv",
        output_file="find_text.csv"
    )
    
    if success:
        print("\nExtraction completed successfully")