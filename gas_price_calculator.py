import os
import json
import re
import pandas as pd
import traceback
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Assistant configuration
ASSISTANT_NAME = "Price of Gas Calculator"
MODEL = "gpt-4o"

# Path for tracking processed contracts
TRACKING_FILE = "processed_contracts.json"

def create_gas_price_assistant():
    """Create a new OpenAI assistant for gas price calculations with the updated prompt"""
    
    assistant_prompt = """You are a financial assistant designed to calculate the average gas price based on historical and forecasted indexed gas prices. You will receive the following inputs:

Price Date (the reference date for all indexed price values)

Contract Duration (in months)

Start Supply Month Index (e.g. January, March, May, etc.)

Indexed Prices available on the given Price Date

🎯 Your Objective
Analyze the data, map each month of the contract to the correct gas price index dont show the thinking, and use Python code to calculate the weighted average price (EUR/MWh).

You must not run the code more than 2 times if the first are the same then output the result.

🔢 Expected Output Format
Return your answer in exactly this JSON format, with price rounded to 2 decimal places:

json
Copy
Edit
{
  "price": "XX.XX EUR/MWh"
}

📘 Rules & Calculation Logic
Determine the contract months from the given start month and duration.

Assign indexes:

In most cases give priority to GQES_Q+X

If the month of  the Price Date is in one of the months: January, April, July, or October that are the start quater months we must always use the GQES_Q+1.

The indexes are always realted to the month of the Price Date.
Example: 16/06/2024, month June -> index GMAES: July, GMES_M+2: August, GQES_Q+1: Jul-Sep, GYES_Y+1: year 2025. If the Start supply month is GMES_M+3 and there is still one month left and we dont have the GMES_M+4 we will need to use GQES_Q+X times the number of months in that quarter.

Use GQES_Q+X only if it fully covers the entire quarter (e.g., Jan–Mar, Apr–Jun, Jul–Sep, Oct–Dec only this examples). Basically everytime the start month supply is Jan, Apr, Jul or Oct you should always use GQES_Q+X

If the starting month is January, April, July, or October, you always use the GQES_Q+X instead of the GMES.

For any missing months, use GYES_Y+X for annual coverage, but only when all quarterly options are exhausted.

Also when a contract duration is more than 24 months we need to be careful because we have no information of the third year we only have information of the year we are currently in plus 2. Example 2023->2025 is the max information we have, for this cases is better to use the GYES_Y+2 for the months of the year+3.

Use GMES_M+X and GMAES for monthly pricing if available. If Start supply month is November 2024 the GMAES is December 2024, GMES_M+2 January 2025 and GMES_M+3 February 2025. GMAES stands for Gas Month Ahead ESpana, so month ahead is basically a M+1 just in a different index, be careful with it. 

You need to make sure to identify the correct index of the start supply month example if we check the price date in September 2024 and the start supply month is January 2025 to Dec 2025 we know that we cannot use GQES_Q+1 because that will be the Oct-Dec 2024 so we use the GYES_Y+1, that is the year 2025.

Make sure the total months in the weights add up exactly to the contract duration VERY IMPORTANT.
Also you should always prioritize the GQES that means the quarters, And if you have available the GQES_Q+1 you should use it the only thing is that if you have 2 months available alone (See example 5) you must pass the GQES_Q+1 because it is redundant it will have one more month that wasnt supposed to.

The GQES_Q+1 must be skipped as in example 4 if the Price Date differs 2 or more months from the Start Supply Month Index.

Use Python code to calculate a weighted average:

python
Copy
Edit
weights = {
  "INDEX_NAME": (value, months_covered),
  ...
}
average = round(sum(value * months for value, months in weights.values()) / total_months, 2)

Ensure that the sum of all weighted months = contract duration.

✅ Example 1
Input:

Price Date: 05/04/2024

Total Duration: 24 months

Start Supply Month: June (GMES_M+2)

Indexed Prices:

rust
Copy
Edit
GMES_M+2 -> 26.50
GQES_Q+1 -> 27.15
GQES_Q+2 -> 29.72
GYES_Y+1 -> 30.60
GYES_Y+2 -> 28.57
Calculation:

June 2024 → GMES_M+2 = 26.50

Jul–Sep 2024 → GQES_Q+1 = 27.15

Oct–Dec 2024 → GQES_Q+2 = 29.72

Jan–Dec 2025 → GYES_Y+1 = 30.60

Jan–May 2026 → GYES_Y+2 = 28.57

python
Copy
Edit
weights = {
  "GMES_M+2": (26.50, 1),
  "GQES_Q+1": (27.15, 3),
  "GQES_Q+2": (29.72, 3),
  "GYES_Y+1": (30.60, 12),
  "GYES_Y+2": (28.57, 5),
}
average = round(sum(v * m for v, m in weights.values()) / 24, 2)
Output:

json
Copy
Edit
{
  "price": "29.47 EUR/MWh"
}
✅ Example 2
Input:

Price Date: 03/05/2024

Total Duration: 24 months

Start Supply Month: June (GMAES)

Indexed Prices:

rust
Copy
Edit
GMAES -> 30.13
GQES_Q+1 -> 30.61
GQES_Q+2 -> 33.99
GYES_Y+1 -> 33.92
GYES_Y+2 -> 28.59
Calculation:

June 2024 → GMAES = 30.13

Jul–Sep 2024 → GQES_Q+1 = 30.61

Oct–Dec 2024 → GQES_Q+2 = 33.99

Jan–Dec 2025 → GYES_Y+1 = 33.92

Jan–May 2026 → GYES_Y+2 = 28.59

python
Copy
Edit
weights = {
  "GMAES": (30.13, 1),
  "GQES_Q+1": (30.61, 3),
  "GQES_Q+2": (33.99, 3),
  "GYES_Y+1": (33.92, 12),
  "GYES_Y+2": (28.59, 5),
}
average = round(sum(v * m for v, m in weights.values()) / 24, 2)
Output:

json
Copy
Edit
{
  "price": "32.25 EUR/MWh"
}
✅ Example 3
Input:

Price Date: 15/03/2024

Total Duration: 12 months

Start Supply Month: May (GMES_M+2)

Indexed Prices:

rust
Copy
Edit
GMES_M+2 -> 26.92
GMES_M+3 -> 27.02
GQES_Q+2 -> 27.30
GQES_Q+3 -> 29.98
GQES_Q+4 -> 31.07
GYES_Y+1 -> 29.65
Calculation:

May 2024 → GMES_M+2 = 26.92

June 2024 → GMES_M+3 = 27.02

Jul–Sep 2024 → GQES_Q+2 = 27.30

Oct–Dec 2024 → GQES_Q+3 = 29.98

Jan–Mar 2025 → GQES_Q+4 = 31.07

April 2025 → GYES_Y+1 = 29.65

python
Copy
Edit
weights = {
  "GMES_M+2": (26.92, 1),
  "GMES_M+3": (27.02, 1),
  "GQES_Q+2": (27.30, 3),
  "GQES_Q+3": (29.98, 3),
  "GQES_Q+4": (31.07, 3),
  "GYES_Y+1": (29.65, 1),
}
The GQES_Q+1 is skipped because the "Price Date":"March" differs 2 or more months from the "Start Supply Month Index": "May".

average = round(sum(v * m for v, m in weights.values()) / 12, 2)
Output:

json
Copy
Edit
{
  "price": "29.05 EUR/MWh"
}
✅ Example 4
Input:

Price Date: 18/04/2024

Total Duration: 12 months

Start Supply Month: June (GMES_M+2)

Indexed Prices:

rust
Copy
Edit
GMES_M+2 -> 31,83
GQES_Q+1 -> 32,48
GQES_Q+2 -> 35,69
GQES_Q+3 -> 37,53
GQES_Q+4 -> 33,94
Calculation:

June 2024 → GMES_M+2 = 31,83

Jul–Sep 2024 → GQES_Q+1 = 32,48

Oct–Dec 2024 → GQES_Q+2 = 35,69

Jan–Mar 2025 → GQES_Q+3 = 37,53

Apr-May 2025 → GQES_Q+4 = 33,94 (If the GQES_Q+4 is available we use it to 

)

python
Copy
Edit
weights = {
  "GMES_M+2": (31,83, 1),
  "GQES_Q+1": (32,48, 3),
  "GQES_Q+2": (35,69, 3),
  "GQES_Q+3": (37,53, 3),
  "GQES_Q+4": (33,94, 2),
}
If there are GQES left you should use them just for cases that we only have two months for example we have GQES_Q+3 for the months Jan, Feb, Mar 2025 and then the GQES_Q+4 since we have it for the months April and May but we multiply by 2 because there are only 2 months. This must only happen if the "Start Supply Month Index" is in the first semester of the year Jan-June because if we are in the second Jul-Dec we must use the GYES_Y+1 multiplied by the numbers of months there is supply in that year. See the next example for that cases.
average = round(sum(v * m for v, m in weights.values()) / 12, 2)
Output:

json
Copy
Edit
{
  "price": " 34,73 EUR/MWh"
}

✅ Example 5
Input:

Price Date: 24/05/2024

Total Duration: 12 months

Start Supply Month: August 

In this cases we always better use GQES_Q+X.
(GMES_M+3, and since we will still need to cover September be careful in this cases)
This will be the case were the month of the supply is M+3 because is 3 months different from the Price date month: May->August but since we dont have M+4 we need to use the quarter example GQES_Q+1 but only times 2 because there is the months August and September only.

Indexed Prices:

rust
Copy
Edit
GQES_Q+1 -> 34,50
GQES_Q+2 -> 37,59
GYES_Y+1 -> 37,20
Calculation:

August, September 2024 → GQES_Q+1 = 34,50

Oct–Dec 2024 → GQES_Q+2 = 37,59

Jan–Jul 2025 → GYES_Y+1 = 37,20

python
Copy
Edit
weights = {
  "GQES_Q+1": (34,50, 2),
  "GQES_Q+2": (37,59, 3),
  "GYES_Y+1": (37,20, 7),
}
Since the start month of supply is plus 3 months and we dont have the M+4 we need to use GQES_Q+1 we dont have other option. If the month of the start of the supply was M+2 then we used GMES_M+2 and GMES_M+3 (Exactly like example 3).
And for the 7 months of 2025 we must use just the GYES_Y+1 times the number of months 7 because we are in the second semester and there is a month left from the calculations July.
average = round(sum(v * m for v, m in weights.values()) / 24, 2)
Output:

json
Copy
Edit
{
  "price": "36,85 EUR/MWh"

🧠 Final Instructions
Use Python code inside the code interpreter to determine applicable months and map them to indexes

Weight each index based on the total duration of the supply always it must be the same as the number of months.

Calculate the weighted average

Return a valid JSON output"""

    try:
        assistant = client.beta.assistants.create(
            name=ASSISTANT_NAME,
            instructions=assistant_prompt,
            model=MODEL,
            tools=[{"type": "code_interpreter"}]
        )
        print(f"Created new assistant: {assistant.id}")
        return assistant.id
    except Exception as e:
        print(f"Error creating assistant: {e}")
        return None

def read_contract_data(contract_id, input_price_date=None, year=None):
    """Read contract data from CSV and Excel files"""
    # If year is explicitly provided, use it directly
    if year:
        pass
    elif input_price_date:
        try:
            for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']:
                try:
                    date_obj = datetime.strptime(input_price_date, fmt)
                    year = date_obj.year
                    break
                except ValueError:
                    continue
            else:
                print(f"Warning: Could not parse date '{input_price_date}', using current year")
                year = datetime.now().year
        except Exception as e:
            print(f"Error parsing date: {e}, using current year")
            year = datetime.now().year
    else:
        year = datetime.now().year

    # Ensure year is only 2023 or 2024
    if year not in [2023, 2024]:
        print(f"Warning: Year {year} is not supported. Using 2024 as default.")
        year = 2024
    
    # Map year to appropriate filenames
    if year == 2023:
        excel_file = "MIBGAS_Data_2023.xlsx"
        csv_file = "output_23_pgas.csv"
    else:
        excel_file = "MIBGAS_Data_2024.xlsx"
        csv_file = "output_24_pgas.csv"
    
    print(f"Using files for year {year}: {csv_file} and {excel_file}")
    
    # Read the CSV file
    print(f"Reading {csv_file}...")
    try:
        df_csv = pd.read_csv(csv_file, sep=";", encoding='utf-8')
    except Exception as e:
        print(f"Error with semicolon separator: {e}")
        df_csv = pd.read_csv(csv_file, encoding='utf-8')
    
    # Find the contract by N.º CONCURSO
    contract_rows = df_csv[df_csv['N.º CONCURSO'] == contract_id]
    if len(contract_rows) == 0:
        try:
            contract_num = int(contract_id)
            if 1 <= contract_num <= len(df_csv):
                contract = df_csv.iloc[contract_num - 1]
                print(f"Contract ID not found, using row {contract_num} instead")
            else:
                raise ValueError(f"Contract number out of range: 1-{len(df_csv)}")
        except ValueError:
            raise ValueError(f"Contract ID '{contract_id}' not found in {csv_file}")
    else:
        contract = contract_rows.iloc[0]
        print(f"Found contract {contract_id} in row {contract_rows.index[0] + 1}")
    
    # Extract required fields
    price_date = contract.get('Price Date', None)
    
    if not price_date or str(price_date).strip() in ["", "-", "nan", "None"]:
        print(f"Warning: Contract {contract_id} has no valid Price Date ({price_date}). Skipping.")
        return {
            'price_date': None,
            'contract_duration': None,
            'start_supply_month': None,
            'price_indices': None
        }
    
    if input_price_date and price_date != input_price_date:
        print(f"Warning: Using provided Price Date '{input_price_date}' instead of CSV value '{price_date}'")
        price_date = input_price_date
        
    contract_duration = contract.get('PRAZOS CONTRATUAIS.DE FORNECIMENTO', None)
    start_supply_month = contract.get('Start supply month', None)
    
    # Read the Excel file for price indices
    print(f"Reading {excel_file}...")
    try:
        if not os.path.exists(excel_file):
            print(f"ERROR: Excel file {excel_file} not found!")
            return {
                'price_date': price_date,
                'contract_duration': contract_duration,
                'start_supply_month': start_supply_month,
                'price_indices': None
            }
        
        # Read Excel file
        df_excel = pd.read_excel(excel_file, sheet_name='Trading Data PVB&VTP')
        
        # Convert price_date string to datetime for comparison
        if price_date:
            try:
                price_date_dt = pd.to_datetime(price_date, format='%d/%m/%Y')
            except:
                try:
                    price_date_dt = pd.to_datetime(price_date, format='%d-%m-%Y')
                except:
                    try:
                        price_date_dt = pd.to_datetime(price_date)
                    except:
                        print(f"Warning: Could not parse date '{price_date}'")
                        price_date_dt = None
            
            # Find matching trading day
            matching_prices = None
            if price_date_dt:
                # Find trading day column
                trading_day_col = None
                for col in df_excel.columns:
                    if 'trading day' in col.lower():
                        trading_day_col = col
                        break
                
                if trading_day_col is None:
                    print("ERROR: 'Trading Day' column not found in Excel sheet!")
                    return {
                        'price_date': price_date,
                        'contract_duration': contract_duration,
                        'start_supply_month': start_supply_month,
                        'price_indices': None
                    }
                
                # Convert Excel dates to datetime
                df_excel[trading_day_col] = pd.to_datetime(df_excel[trading_day_col], errors='coerce')
                
                # Find exact match first
                matching_row = df_excel[df_excel[trading_day_col] == price_date_dt]
                
                # If no exact match, find closest date before
                if matching_row.empty:
                    print(f"No exact match for {price_date_dt}, looking for closest date...")
                    earlier_dates = df_excel[df_excel[trading_day_col] < price_date_dt]
                    if not earlier_dates.empty:
                        closest_date = earlier_dates[trading_day_col].max()
                        matching_row = df_excel[df_excel[trading_day_col] == closest_date]
                        print(f"Using data from closest date: {closest_date}")
                
                if not matching_row.empty:
                    # Find product and price columns
                    product_col = None
                    for col in df_excel.columns:
                        if any(p.lower() in col.lower() for p in ['Product', 'Code', 'product', 'code']):
                            product_col = col
                            break
                    
                    # Use 9th column (column I) for prices
                    price_col = df_excel.columns[8]
                    
                    if product_col:
                        # Create dictionary of Product -> Last Price
                        matching_prices = dict(zip(matching_row[product_col], matching_row[price_col]))
                        
                        # Remove NaN values and empty entries
                        matching_prices = {k: v for k, v in matching_prices.items() 
                                         if pd.notna(k) and pd.notna(v) and str(k).strip() != '' and str(v).strip() != ''}
                        
                        # Filter for relevant indices (monthly/quarterly/yearly, not daily)
                        relevant_indices = {}
                        for code, value in matching_prices.items():
                            code_str = str(code).upper()
                            # Look for monthly, quarterly, and yearly indices
                            if any(pattern in code_str for pattern in ['GMAES', 'GMES', 'GQES', 'GYES']):
                                relevant_indices[code] = value
                        
                        print(f"Found {len(relevant_indices)} relevant monthly/quarterly/yearly indices")
                        
                        # Check if we have fewer than 8 relevant indices
                        if len(relevant_indices) < 8:
                            print(f"Found only {len(relevant_indices)} relevant indices, looking 2 days before...")
                            
                            # Look for data 2 days before
                            two_days_before = price_date_dt - timedelta(days=2)
                            matching_row_2days = df_excel[df_excel[trading_day_col] == two_days_before]
                            
                            if matching_row_2days.empty:
                                # If no exact match 2 days before, find closest date before that
                                earlier_dates_2days = df_excel[df_excel[trading_day_col] < two_days_before]
                                if not earlier_dates_2days.empty:
                                    closest_date_2days = earlier_dates_2days[trading_day_col].max()
                                    matching_row_2days = df_excel[df_excel[trading_day_col] == closest_date_2days]
                                    print(f"Using data from closest date 2 days before: {closest_date_2days}")
                            
                            if not matching_row_2days.empty:
                                # Get prices from 2 days before
                                matching_prices_2days = dict(zip(matching_row_2days[product_col], matching_row_2days[price_col]))
                                matching_prices_2days = {k: v for k, v in matching_prices_2days.items() 
                                                        if pd.notna(k) and pd.notna(v) and str(k).strip() != '' and str(v).strip() != ''}
                                
                                # Filter for relevant indices from 2 days before
                                relevant_indices_2days = {}
                                for code, value in matching_prices_2days.items():
                                    code_str = str(code).upper()
                                    if any(pattern in code_str for pattern in ['GMAES', 'GMES', 'GQES', 'GYES']):
                                        relevant_indices_2days[code] = value
                                
                                print(f"Found {len(relevant_indices_2days)} relevant indices from 2 days before")
                                
                                # Use the data from 2 days before if it has more relevant indices
                                if len(relevant_indices_2days) >= 8:
                                    relevant_indices = relevant_indices_2days
                                    matching_prices = matching_prices_2days
                                    print("Using price data from 2 days before (has >= 8 relevant indices)")
                                elif len(relevant_indices_2days) > len(relevant_indices):
                                    relevant_indices = relevant_indices_2days
                                    matching_prices = matching_prices_2days
                                    print(f"Using price data from 2 days before (has more relevant indices: {len(relevant_indices_2days)} vs {len(relevant_indices)})")
                                else:
                                    print(f"Data from 2 days before doesn't have more relevant indices ({len(relevant_indices_2days)}), using original data")
                        
                        # Use all matching prices (including daily ones) for the final output
                        # but prioritize the relevant ones
                        if relevant_indices:
                            # If we have relevant indices, include them plus any other indices
                            final_prices = matching_prices
                        else:
                            # If no relevant indices found, use all available
                            final_prices = matching_prices
                            print("Warning: No monthly/quarterly/yearly indices found, using all available indices")
                        
                        # Format prices with comma as decimal separator
                        final_prices = {k: str(v).replace('.', ',') for k, v in final_prices.items()}
                        
                        print(f"Final: Found {len(final_prices)} total price indices ({len(relevant_indices)} relevant)")
                        matching_prices = final_prices
        
        return {
            'price_date': price_date,
            'contract_duration': contract_duration,
            'start_supply_month': start_supply_month,
            'price_indices': matching_prices
        }
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        traceback.print_exc()
        return {
            'price_date': price_date,
            'contract_duration': contract_duration,
            'start_supply_month': start_supply_month,
            'price_indices': None
        }

def create_prompt_from_contract(contract_data):
    """Create prompt from contract data"""
    price_date = contract_data.get('price_date', 'N/A')
    contract_duration = contract_data.get('contract_duration', 'N/A')
    start_supply_month = contract_data.get('start_supply_month', 'N/A')
    price_indices = contract_data.get('price_indices', {})
    
    # Format the price indices
    indices_text = "\n".join([f"{code} -> {value}" for code, value in price_indices.items()]) if price_indices else "No price indices found"
    
    prompt = f"""
Input:
Price Date: {price_date} 
Total duration of the contract: {contract_duration}
Start supply month index: {start_supply_month}

Price Index for that day
{indices_text}
"""
    return prompt

def get_assistant_response(prompt, assistant_id):
    """Query AI Assistant"""
    print("Creating thread...")
    
    # Create thread and send message
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=prompt
    )

    print(f"Running assistant {assistant_id}...")
    
    # Run assistant
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id
    )

    # Wait for completion
    print("Waiting for assistant response...")
    while run.status not in ["completed", "failed", "cancelled", "expired"]:
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id
        )
        print(f"Run status: {run.status}")
        if run.status == "failed":
            print(f"Run failed with error: {run.last_error}")
            return None
        import time
        time.sleep(1)

    # Get response
    print("Getting response...")
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    
    for msg in messages.data:
        if hasattr(msg, 'content') and msg.content:
            for content_item in msg.content:
                if hasattr(content_item, 'text'):
                    return content_item.text.value
    
    print("No text content found in the response")
    return None

def extract_price(response):
    """Extract price from response"""
    if not response:
        return "No response to extract price from"
    
    # Try to parse as JSON
    try:
        json_match = re.search(r'\{.*"price".*\}', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            data = json.loads(json_str)
            if 'price' in data:
                price_value = data.get('price')
                return clean_price_value(price_value)
    except json.JSONDecodeError:
        pass
    
    # Try different regex patterns
    price_match = re.search(r'price"?\s*:?\s*["\']?(\d+[,.]\d+)["\']?', response, re.IGNORECASE)
    if price_match:
        return clean_price_value(price_match.group(1))
    
    eur_match = re.search(r'(\d+[,.]\d+)\s*EUR\/MWh', response, re.IGNORECASE)
    if eur_match:
        return clean_price_value(eur_match.group(1))
    
    generic_price = re.search(r'(\d+[,.]\d+)', response)
    if generic_price:
        return clean_price_value(generic_price.group(1))
    
    return "Could not extract price"

def clean_price_value(price_str):
    """Clean price values"""
    price_str = str(price_str)
    
    numeric_match = re.search(r'(\d+[.,]\d+)', price_str)
    if numeric_match:
        numeric_value = numeric_match.group(1)
        if '.' in numeric_value:
            numeric_value = numeric_value.replace('.', ',')
        return numeric_value
    
    whole_number_match = re.search(r'(\d+)', price_str)
    if whole_number_match:
        return whole_number_match.group(1)
        
    print(f"Warning: Could not extract numeric value from '{price_str}'")
    return price_str

def process_contract(contract_id, year, price_date=None, assistant_id=None):
    """Process a single contract"""
    print(f"\n{'='*50}")
    print(f"Processing contract: {contract_id} (Year: {year})")
    print(f"{'='*50}")
    
    try:
        # Determine CSV file
        csv_file = "output_23_pgas.csv" if year == 2023 else "output_24_pgas.csv"
            
        # Read contract data
        contract_data = read_contract_data(contract_id, price_date, year)
        
        # Validate required fields
        if not contract_data.get('price_date') or contract_data['price_date'].strip() in ["", "-"]:
            print("Error: Price Date is empty or invalid. Skipping.")
            return False
        
        if not contract_data.get('contract_duration') or str(contract_data['contract_duration']).strip() in ["", "-", "nan", "None"]:
            print("Error: Contract Duration is empty or invalid. Skipping.")
            return False
        
        if not contract_data.get('start_supply_month') or str(contract_data['start_supply_month']).strip() in ["", "-", "nan", "None"]:
            print("Error: Start Supply Month is empty or invalid. Skipping.")
            return False
        
        # Create prompt
        prompt = create_prompt_from_contract(contract_data)
        print("\nGenerated prompt:")
        print(prompt)
        
        # Confirm before sending
        confirm = input("\nSend this to OpenAI Assistant? (yes/no): ").lower().strip()
        if confirm not in ['yes', 'y']:
            print("Operation cancelled by user.")
            return False
        
        print("\nSending to OpenAI Assistant...")
        response = get_assistant_response(prompt, assistant_id)
        
        if response:
            print("\nAssistant Response:")
            print("-" * 50)
            print(response)
            print("-" * 50)
            
            # Extract price
            price = extract_price(response)
            print(f"\nFinal Price: {price}")
            
            # Update CSV
            try:
                df_csv = pd.read_csv(csv_file, sep=";", encoding='utf-8')
                
                # Find contract row
                mask = df_csv['N.º CONCURSO'] == contract_id
                if not mask.any():
                    try:
                        row_index = int(contract_id) - 1
                        if 0 <= row_index < len(df_csv):
                            mask = [False] * len(df_csv)
                            mask[row_index] = True
                    except ValueError:
                        print(f"Warning: Could not find contract {contract_id} in {csv_file}")
                        return True
                
                # Add Fixed price column if needed
                if 'Fixed price' not in df_csv.columns:
                    df_csv['Fixed price'] = None
                
                # Update price
                df_csv.loc[mask, 'Fixed price'] = price
                df_csv.to_csv(csv_file, sep=";", index=False, encoding='utf-8')
                print(f"\nUpdated {csv_file} with calculated price for contract {contract_id}")
            except Exception as e:
                print(f"Warning: Could not update CSV file with price: {e}")
            
            return True
        else:
            print("\nNo response received from assistant.")
            return False
    
    except Exception as e:
        print(f"Error processing contract: {e}")
        traceback.print_exc()
        return False

def main():
    """Main execution function"""
    print("Gas Price Calculator")
    print("=" * 50)
    
    # Always create a new assistant with the updated prompt
    print("Creating new OpenAI Assistant with updated prompt...")
    assistant_id = create_gas_price_assistant()
    
    if not assistant_id:
        print("Failed to create assistant. Exiting.")
        return
    
    print(f"New assistant created successfully: {assistant_id}")
    print("This assistant will be used for all calculations in this session.")
    
    # Ask for processing mode
    print("\nChoose processing mode:")
    print("1. Process a single contract")
    print("2. Process all contracts from 2023")
    print("3. Process all contracts from 2024")
    
    mode = input("Enter mode (1-3): ").strip()
    
    if mode == "1":
        # Process single contract
        year_input = input("Enter year (2023/2024): ")
        try:
            year = int(year_input)
            if year not in [2023, 2024]:
                print("Invalid year. Using 2024 as default.")
                year = 2024
        except:
            print("Invalid input. Using 2024 as default.")
            year = 2024
            
        csv_file = "output_23_pgas.csv" if year == 2023 else "output_24_pgas.csv"
        print(f"\nUsing {csv_file} for year {year}")
        
        # Show available contracts
        try:
            df_csv = pd.read_csv(csv_file, sep=";", encoding='utf-8')
            print("\nAvailable contracts:")
            for idx, row in df_csv.iterrows():
                price_date = row.get('Price Date', '-')
                if pd.isna(price_date) or str(price_date).strip() in ["", "-", "nan", "None"]:
                    continue
                contract_id = row.get('N.º CONCURSO', str(idx + 1))
                name = row.get('NOME', '-')
                print(f"Contract ID: {contract_id}, Name: {name}, Price Date: {price_date}")
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            return
        
        contract_id = input("\nEnter contract ID: ")
        price_date = input("Enter Price Date (DD/MM/YYYY) or leave empty: ")
        if price_date.strip() == "":
            price_date = None
        
        # Process contract
        process_contract(contract_id, year, price_date, assistant_id)
    
    elif mode in ["2", "3"]:
        year = 2023 if mode == "2" else 2024
        print(f"Batch processing for year {year} not implemented in this version.")
        print("Please use mode 1 to process individual contracts.")
    
    else:
        print("Invalid mode selected.")

if __name__ == "__main__":
    main() 