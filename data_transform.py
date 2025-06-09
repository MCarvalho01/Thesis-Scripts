"""
Gas Market Analysis Toolkit

A comprehensive toolkit for analyzing gas price contracts, calculating margins,
and performing financial analysis on energy market data. This toolkit combines
multiple analytical functions for processing contract data, price calculations,
and competitive analysis.

Author: Energy Market Analysis Team
Version: 1.0
Date: 2024
"""

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

class GasMarketAnalyzer:
    """
    Main class for gas market analysis operations including price calculations,
    margin analysis, and competitive assessments.
    """
    
    def __init__(self, openai_api_key=None):
        """Initialize the analyzer with optional OpenAI API key."""
        if openai_api_key:
            self.client = OpenAI(api_key=openai_api_key)
        else:
            self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # AI Assistant configuration
        self.assistant_name = "Gas Price Calculator"
        self.model = "gpt-4o"
        
        # Price index types configuration
        self.index_hierarchy = {
            'monthly': ['GMAES', 'GMES_M'],
            'quarterly': ['GQES_Q'],
            'annual': ['GYES_Y']
        }
    
    def clean_numeric_value(self, value):
        """
        Convert various string formats to numeric values.
        
        Args:
            value: Input value in various formats (string, number, etc.)
            
        Returns:
            float: Cleaned numeric value or None if invalid
        """
        if pd.isna(value) or value in ['-', '', 'None', 'nan', 'NAP']:
            return None
        
        if isinstance(value, str):
            # Skip values with quarterly prefixes that indicate incomplete data
            if re.search(r'1\.º\s*Trim|1º\s*Trim', value):
                return None
            
            # Handle k= prefix for index values
            k_match = re.search(r'k=(\d+[.,]\d+)', value)
            if k_match:
                value = k_match.group(1)
            
            # Handle commas as decimal separators
            value = value.replace(',', '.')
            
            # Extract numeric part
            match = re.search(r'[-+]?\d*\.\d+|\d+', value)
            if match:
                return float(match.group())
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def extract_index_coefficient(self, value):
        """
        Extract coefficient value from index strings (e.g., k=0.045).
        
        Args:
            value: String containing index coefficient
            
        Returns:
            float: Coefficient value or None if not found
        """
        if pd.isna(value) or not isinstance(value, str):
            return None
        
        # Skip quarterly prefix values
        if re.search(r'1\.º\s*Trim|1º\s*Trim', value):
            return None
        
        k_match = re.search(r'k=\s*([-+]?\d*[.,]?\d+)', value)
        if k_match:
            k_value = k_match.group(1).replace(',', '.')
            try:
                return float(k_value)
            except (ValueError, TypeError):
                return None
        return None
    
    def create_price_calculation_assistant(self):
        """
        Create OpenAI assistant for gas price calculations with specialized prompt.
        
        Returns:
            str: Assistant ID or None if creation failed
        """
        assistant_prompt = """You are a financial assistant designed to calculate the average gas price based on historical and forecasted indexed gas prices. You will receive the following inputs:

Price Date (the reference date for all indexed price values)
Contract Duration (in months)
Start Supply Month Index (e.g. January, March, May, etc.)
Indexed Prices available on the given Price Date

🎯 Your Objective
Analyze the data, map each month of the contract to the correct gas price index, and use Python code to calculate the weighted average price (EUR/MWh).

🔢 Expected Output Format
Return your answer in exactly this JSON format, with price rounded to 2 decimal places:
{
  "price": "XX.XX EUR/MWh"
}

📘 Rules & Calculation Logic
1. Determine contract months from start month and duration
2. Assign indexes with priority hierarchy:
   - Primary: GQES_Q+X (Quarterly indices)
   - Secondary: GMES_M+X and GMAES (Monthly indices)
   - Fallback: GYES_Y+X (Annual indices)

3. Special rules:
   - If Price Date month ∈ {January, April, July, October}, always use GQES_Q+1
   - Skip GQES_Q+1 if Price Date differs ≥2 months from Start Supply Month
   - Use GQES_Q+X only for complete quarters
   - For contracts >24 months: Use GYES_Y+2 for year+3 months

4. Calculate weighted average:
weights = {
  "INDEX_NAME": (price_value, months_covered),
  ...
}
average = round(sum(value * months for value, months in weights.values()) / total_months, 2)

5. Ensure sum of weighted months = contract duration

🧠 Final Instructions
- Use Python code to determine applicable months and map to indexes
- Weight each index based on duration coverage
- Calculate weighted average
- Return valid JSON output"""

        try:
            assistant = self.client.beta.assistants.create(
                name=self.assistant_name,
                instructions=assistant_prompt,
                model=self.model,
                tools=[{"type": "code_interpreter"}]
            )
            print(f"Created assistant: {assistant.id}")
            return assistant.id
        except Exception as e:
            print(f"Error creating assistant: {e}")
            return None
    
    def read_contract_data(self, contract_id, input_price_date=None, year=None, csv_file=None, excel_file=None):
        """
        Read contract data from CSV and Excel files.
        
        Args:
            contract_id: Contract identifier
            input_price_date: Override price date
            year: Data year (2023 or 2024)
            csv_file: Path to CSV file
            excel_file: Path to Excel file
            
        Returns:
            dict: Contract data including price indices
        """
        # Determine year from price date if not provided
        if year is None and input_price_date:
            try:
                for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']:
                    try:
                        date_obj = datetime.strptime(input_price_date, fmt)
                        year = date_obj.year
                        break
                    except ValueError:
                        continue
                else:
                    year = datetime.now().year
            except:
                year = datetime.now().year
        elif year is None:
            year = datetime.now().year
        
        # Default file paths if not provided
        if csv_file is None:
            csv_file = f"output_{str(year)[2:]}_pgas.csv"
        if excel_file is None:
            excel_file = f"MIBGAS_Data_{year}.xlsx"
        
        try:
            # Read CSV contract data
            df_csv = pd.read_csv(csv_file, sep=";", encoding='utf-8')
            
            # Find contract
            contract_rows = df_csv[df_csv['N.º CONCURSO'] == contract_id]
            if len(contract_rows) == 0:
                try:
                    contract_num = int(contract_id)
                    if 1 <= contract_num <= len(df_csv):
                        contract = df_csv.iloc[contract_num - 1]
                    else:
                        raise ValueError(f"Contract number out of range")
                except ValueError:
                    raise ValueError(f"Contract ID '{contract_id}' not found")
            else:
                contract = contract_rows.iloc[0]
            
            # Extract contract fields
            price_date = contract.get('Price Date', None)
            if input_price_date and price_date != input_price_date:
                price_date = input_price_date
            
            contract_duration = contract.get('PRAZOS CONTRATUAIS.DE FORNECIMENTO', None)
            start_supply_month = contract.get('Start supply month', None)
            
            # Validate required fields
            if not price_date or str(price_date).strip() in ["", "-", "nan", "None"]:
                return self._create_empty_contract_data(price_date, contract_duration, start_supply_month)
            
            # Read Excel price indices
            price_indices = self._read_price_indices(excel_file, price_date)
            
            return {
                'price_date': price_date,
                'contract_duration': contract_duration,
                'start_supply_month': start_supply_month,
                'price_indices': price_indices
            }
            
        except Exception as e:
            print(f"Error reading contract data: {e}")
            traceback.print_exc()
            return self._create_empty_contract_data(None, None, None)
    
    def _create_empty_contract_data(self, price_date, contract_duration, start_supply_month):
        """Create empty contract data structure."""
        return {
            'price_date': price_date,
            'contract_duration': contract_duration,
            'start_supply_month': start_supply_month,
            'price_indices': None
        }
    
    def _read_price_indices(self, excel_file, price_date):
        """
        Read price indices from Excel file for given price date.
        
        Args:
            excel_file: Path to Excel file
            price_date: Target price date
            
        Returns:
            dict: Price indices for the date
        """
        try:
            if not os.path.exists(excel_file):
                print(f"Excel file {excel_file} not found")
                return None
            
            # Read Excel file
            df_excel = pd.read_excel(excel_file, sheet_name='Trading Data PVB&VTP')
            
            # Convert price date
            price_date_dt = pd.to_datetime(price_date, format='%d/%m/%Y')
            
            # Find trading day column
            trading_day_col = None
            for col in df_excel.columns:
                if 'trading day' in col.lower():
                    trading_day_col = col
                    break
            
            if trading_day_col is None:
                print("Trading Day column not found")
                return None
            
            # Convert Excel dates
            df_excel[trading_day_col] = pd.to_datetime(df_excel[trading_day_col], errors='coerce')
            
            # Find matching date or closest before
            matching_row = df_excel[df_excel[trading_day_col] == price_date_dt]
            
            if matching_row.empty:
                earlier_dates = df_excel[df_excel[trading_day_col] < price_date_dt]
                if not earlier_dates.empty:
                    closest_date = earlier_dates[trading_day_col].max()
                    matching_row = df_excel[df_excel[trading_day_col] == closest_date]
            
            if not matching_row.empty:
                # Find product and price columns
                product_col = None
                for col in df_excel.columns:
                    if any(p.lower() in col.lower() for p in ['Product', 'Code']):
                        product_col = col
                        break
                
                # Use 9th column for prices
                price_col = df_excel.columns[8]
                
                if product_col:
                    # Create price dictionary
                    matching_prices = dict(zip(matching_row[product_col], matching_row[price_col]))
                    
                    # Filter relevant indices
                    relevant_indices = {}
                    for code, value in matching_prices.items():
                        if pd.notna(code) and pd.notna(value):
                            code_str = str(code).upper()
                            if any(pattern in code_str for pattern in ['GMAES', 'GMES', 'GQES', 'GYES']):
                                relevant_indices[code] = str(value).replace('.', ',')
                    
                    return relevant_indices
            
            return None
            
        except Exception as e:
            print(f"Error reading price indices: {e}")
            return None
    
    def calculate_gas_price(self, contract_data, assistant_id):
        """
        Calculate gas price using AI assistant.
        
        Args:
            contract_data: Contract information dictionary
            assistant_id: OpenAI assistant ID
            
        Returns:
            str: Calculated price or error message
        """
        try:
            # Create prompt
            prompt = self._create_calculation_prompt(contract_data)
            
            # Get AI response
            response = self._get_assistant_response(prompt, assistant_id)
            
            if response:
                # Extract price from response
                price = self._extract_price_from_response(response)
                return price
            else:
                return "No response from assistant"
                
        except Exception as e:
            print(f"Error calculating gas price: {e}")
            return "Calculation error"
    
    def _create_calculation_prompt(self, contract_data):
        """Create prompt for price calculation."""
        price_date = contract_data.get('price_date', 'N/A')
        contract_duration = contract_data.get('contract_duration', 'N/A')
        start_supply_month = contract_data.get('start_supply_month', 'N/A')
        price_indices = contract_data.get('price_indices', {})
        
        indices_text = "\n".join([f"{code} -> {value}" for code, value in price_indices.items()]) if price_indices else "No price indices found"
        
        return f"""
Input:
Price Date: {price_date}
Total duration of the contract: {contract_duration}
Start supply month index: {start_supply_month}

Price Index for that day
{indices_text}
"""
    
    def _get_assistant_response(self, prompt, assistant_id):
        """Get response from OpenAI assistant."""
        try:
            # Create thread
            thread = self.client.beta.threads.create()
            self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=prompt
            )
            
            # Run assistant
            run = self.client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id
            )
            
            # Wait for completion
            while run.status not in ["completed", "failed", "cancelled", "expired"]:
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                if run.status == "failed":
                    return None
                import time
                time.sleep(1)
            
            # Get response
            messages = self.client.beta.threads.messages.list(thread_id=thread.id)
            
            for msg in messages.data:
                if hasattr(msg, 'content') and msg.content:
                    for content_item in msg.content:
                        if hasattr(content_item, 'text'):
                            return content_item.text.value
            
            return None
            
        except Exception as e:
            print(f"Error getting assistant response: {e}")
            return None
    
    def _extract_price_from_response(self, response):
        """Extract price value from assistant response."""
        if not response:
            return "No response to extract price from"
        
        # Try JSON parsing
        try:
            json_match = re.search(r'\{.*"price".*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                if 'price' in data:
                    return self._clean_price_value(data['price'])
        except json.JSONDecodeError:
            pass
        
        # Try regex patterns
        price_match = re.search(r'price"?\s*:?\s*["\']?(\d+[,.]\d+)["\']?', response, re.IGNORECASE)
        if price_match:
            return self._clean_price_value(price_match.group(1))
        
        eur_match = re.search(r'(\d+[,.]\d+)\s*EUR\/MWh', response, re.IGNORECASE)
        if eur_match:
            return self._clean_price_value(eur_match.group(1))
        
        return "Could not extract price"
    
    def _clean_price_value(self, price_str):
        """Clean and format price values."""
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
        
        return price_str
    
    def calculate_profit_margins(self, df):
        """
        Calculate profit margins for contracts.
        
        Args:
            df: DataFrame with contract data
            
        Returns:
            DataFrame: Updated DataFrame with profit margins
        """
        updated_profit_count = 0
        updated_real_profit_count = 0
        
        for idx, row in df.iterrows():
            # Calculate basic profit margin (€/MWh)
            fixed_price = self.clean_numeric_value(row.get('Fixed price'))
            winning_proposal = self.clean_numeric_value(row.get('Proposta_Vencedor'))
            
            if fixed_price is not None and winning_proposal is not None:
                profit_margin = winning_proposal - fixed_price
                formatted_margin = str(round(profit_margin, 3)).replace('.', ',')
                
                current_margin = self.clean_numeric_value(row.get('Profit_Margin (€/MWh)'))
                
                if current_margin is None or abs(current_margin - profit_margin) > 0.001:
                    df.at[idx, 'Profit_Margin (€/MWh)'] = formatted_margin
                    updated_profit_count += 1
            
            # Calculate real profit (€)
            total_consumption = self.clean_numeric_value(row.get('CONSUMO TOTAL.kWh'))
            profit_margin_val = self.clean_numeric_value(row.get('Profit_Margin (€/MWh)'))
            
            if total_consumption is not None and profit_margin_val is not None:
                # Convert kWh to MWh and multiply by margin
                real_profit = (total_consumption / 1000) * profit_margin_val
                formatted_real_profit = str(round(real_profit, 2)).replace('.', ',')
                
                if df.at[idx, 'Real profit'] != formatted_real_profit:
                    df.at[idx, 'Real profit'] = formatted_real_profit
                    updated_real_profit_count += 1
            else:
                if df.at[idx, 'Real profit'] != '-':
                    df.at[idx, 'Real profit'] = '-'
                    updated_real_profit_count += 1
        
        print(f"Updated {updated_profit_count} profit margins and {updated_real_profit_count} real profit values")
        return df
    
    def calculate_competitor_margins(self, df):
        """
        Calculate margins for competitors and reference entity.
        
        Args:
            df: DataFrame with contract data
            
        Returns:
            DataFrame: Updated DataFrame with competitor margins
        """
        # Define competitor columns (generic names)
        competitor_columns = [
            "PROPOSTA CONCORRENTES €/kWh.Company_A",
            "PROPOSTA CONCORRENTES €/kWh.Company_B",
            "PROPOSTA CONCORRENTES €/kWh.Company_C",
            "PROPOSTA CONCORRENTES €/kWh.Company_D",
            "PROPOSTA CONCORRENTES €/kWh.Company_E",
            "PROPOSTA CONCORRENTES €/kWh.Others"
        ]
        
        margin_columns = [
            "Margin_Company_A",
            "Margin_Company_B", 
            "Margin_Company_C",
            "Margin_Company_D",
            "Margin_Company_E",
            "Margin_Others"
        ]
        
        # Add margin columns if they don't exist
        for col in margin_columns + ["Margin_Reference"]:
            if col not in df.columns:
                df[col] = "-"
        
        updates_made = {col: 0 for col in margin_columns + ["Margin_Reference"]}
        
        for idx, row in df.iterrows():
            fixed_price = self.clean_numeric_value(row.get('Fixed price'))
            
            # Process reference entity margin
            reference_proposal = row.get('Reference_Entity_Proposal')
            if reference_proposal and reference_proposal not in ['-', '']:
                # Handle coefficient-based proposals
                if isinstance(reference_proposal, str) and 'k=' in reference_proposal.lower():
                    k_value = self.extract_index_coefficient(reference_proposal)
                    if k_value is not None:
                        formatted_value = f"{k_value:.3f}".replace('.', ',')
                        if row.get("Margin_Reference") != formatted_value:
                            df.at[idx, "Margin_Reference"] = formatted_value
                            updates_made["Margin_Reference"] += 1
                else:
                    # Regular price-based proposals
                    ref_price = self.clean_numeric_value(reference_proposal)
                    if ref_price is not None and fixed_price is not None:
                        # Convert units if needed
                        if ref_price < 1:
                            ref_price_mwh = ref_price * 1000
                        else:
                            ref_price_mwh = ref_price
                        
                        margin = ref_price_mwh - fixed_price
                        formatted_margin = f"{margin:.2f}".replace('.', ',')
                        
                        if row.get("Margin_Reference") != formatted_margin:
                            df.at[idx, "Margin_Reference"] = formatted_margin
                            updates_made["Margin_Reference"] += 1
            
            # Process competitor margins
            for comp_col, margin_col in zip(competitor_columns, margin_columns):
                competitor_value = row.get(comp_col)
                
                if competitor_value and competitor_value not in ['NAP', '-', '']:
                    # Skip quarterly prefix values
                    if isinstance(competitor_value, str) and re.search(r'1\.º\s*Trim|1º\s*Trim', competitor_value):
                        if row.get(margin_col) != "-":
                            df.at[idx, margin_col] = "-"
                            updates_made[margin_col] += 1
                        continue
                    
                    # Handle coefficient values
                    k_value = self.extract_index_coefficient(competitor_value)
                    if k_value is not None:
                        k_value_mwh = k_value * 1000
                        formatted_value = f"{k_value_mwh:.3f}".replace('.', ',')
                        
                        if row.get(margin_col) != formatted_value:
                            df.at[idx, margin_col] = formatted_value
                            updates_made[margin_col] += 1
                    else:
                        # Regular competitor price
                        competitor_price = self.clean_numeric_value(competitor_value)
                        if competitor_price is not None and fixed_price is not None:
                            competitor_price_mwh = competitor_price * 1000
                            margin = abs(competitor_price_mwh - fixed_price)
                            formatted_margin = f"{margin:.2f}".replace('.', ',')
                            
                            if row.get(margin_col) != formatted_margin:
                                df.at[idx, margin_col] = formatted_margin
                                updates_made[margin_col] += 1
        
        # Print update summary
        for col, count in updates_made.items():
            if count > 0:
                print(f"Updated {count} values in {col}")
        
        return df
    
    def process_contract_file(self, csv_file, excel_file=None, save_results=True):
        """
        Process entire contract file with all calculations.
        
        Args:
            csv_file: Path to CSV file
            excel_file: Path to Excel file (optional)
            save_results: Whether to save results
            
        Returns:
            DataFrame: Processed data
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, sep=";", encoding='utf-8')
            print(f"Processing {len(df)} contracts from {csv_file}")
            
            # Apply all calculations
            df = self.calculate_profit_margins(df)
            df = self.calculate_competitor_margins(df)
            
            # Save results if requested
            if save_results:
                df.to_csv(csv_file, sep=";", index=False, encoding='utf-8')
                print(f"Results saved to {csv_file}")
            
            return df
            
        except Exception as e:
            print(f"Error processing contract file: {e}")
            traceback.print_exc()
            return None
    
    def analyze_market_competitiveness(self, df):
        """
        Analyze market competitiveness metrics.
        
        Args:
            df: DataFrame with contract data
            
        Returns:
            dict: Market analysis results
        """
        try:
            # Calculate statistics
            total_contracts = len(df)
            contracts_with_margins = df[df['Profit_Margin (€/MWh)'] != '-'].shape[0]
            
            # Margin distribution
            valid_margins = []
            for margin in df['Profit_Margin (€/MWh)']:
                margin_val = self.clean_numeric_value(margin)
                if margin_val is not None:
                    valid_margins.append(margin_val)
            
            analysis = {
                'total_contracts': total_contracts,
                'contracts_with_margins': contracts_with_margins,
                'margin_coverage': f"{(contracts_with_margins/total_contracts)*100:.1f}%",
                'average_margin': f"{sum(valid_margins)/len(valid_margins):.2f}" if valid_margins else "N/A",
                'min_margin': f"{min(valid_margins):.2f}" if valid_margins else "N/A",
                'max_margin': f"{max(valid_margins):.2f}" if valid_margins else "N/A"
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error in market analysis: {e}")
            return {}


# Utility functions for standalone use
def process_gas_contracts(csv_file, excel_file=None, openai_api_key=None):
    """
    Standalone function to process gas contracts.
    
    Args:
        csv_file: Path to CSV file
        excel_file: Path to Excel file
        openai_api_key: OpenAI API key
        
    Returns:
        DataFrame: Processed contract data
    """
    analyzer = GasMarketAnalyzer(openai_api_key)
    return analyzer.process_contract_file(csv_file, excel_file)


def calculate_single_gas_price(contract_id, csv_file, excel_file, openai_api_key=None):
    """
    Calculate gas price for a single contract.
    
    Args:
        contract_id: Contract identifier
        csv_file: Path to CSV file
        excel_file: Path to Excel file
        openai_api_key: OpenAI API key
        
    Returns:
        str: Calculated price
    """
    analyzer = GasMarketAnalyzer(openai_api_key)
    
    # Create assistant
    assistant_id = analyzer.create_price_calculation_assistant()
    if not assistant_id:
        return "Failed to create assistant"
    
    # Read contract data
    contract_data = analyzer.read_contract_data(contract_id, csv_file=csv_file, excel_file=excel_file)
    
    # Calculate price
    price = analyzer.calculate_gas_price(contract_data, assistant_id)
    return price


if __name__ == "__main__":
    # Example usage
    print("Gas Market Analysis Toolkit")
    print("=" * 50)
    
    # Initialize analyzer
    analyzer = GasMarketAnalyzer()
    
    # Example: Process contract files
    # df = analyzer.process_contract_file("contracts.csv", "market_data.xlsx")
    
    # Example: Analyze market
    # analysis = analyzer.analyze_market_competitiveness(df)
    # print("Market Analysis Results:", analysis)
    
    print("Toolkit ready for use.") 