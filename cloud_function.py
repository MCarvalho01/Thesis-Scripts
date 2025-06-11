import os
import json
import re
import calendar
import pandas as pd
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- COLUMN MAPPING ---
# This dictionary maps between various column name formats to ensure consistency
COLUMN_MAPPING = {
    # Form/CSV name to database column name
    "concursoNum": "concursonum",
    "nome": "nome",
    "dataPublicacao": "datapublicacao",
    "prazoEntregaData": "prazoentregadata",
    "consumoTotalKWh": "consumototalkwh",
    "valorContrato": "valorcontrato",
    "proposto_company_indexante_tipo": "proposto_company_indexante_tipo",
    "proposto_company_indexante_cotacao": "proposto_company_indexante_cotacao",
    "proposto_company_k": "proposto_company_k",
    "propostaCompanyPE": "propostacompanype",
    "fixed_price": "fixed_price",
    "vencedor": "vencedor",
    # Generic competitor proposal columns
    "PROPOSTA_COMPETITOR_A": "proposta_competitor_a",
    "PROPOSTA_COMPETITOR_B": "proposta_competitor_b",
    "PROPOSTA_COMPETITOR_C": "proposta_competitor_c",
    "PROPOSTA_COMPETITOR_D": "proposta_competitor_d",
    "PROPOSTA_COMPETITOR_E": "proposta_competitor_e",
    "proposta_concorrentes_outros": "proposta_concorrentes_outros",
    "source_year": "source_year",
    "referencia": "referencia",
    "proposta_vencedor": "propostavencedor",
    "empresasComPropostas": "empresascompropostas",
    "tipo": "tipo",
    
    # Additional calculated fields
    "Company_Proposal": "company_proposal",
    "Profit_Margin": "profit_margin",
    "Real_profit": "real_profit",
    
    # Margin fields for all competitors
    "Margin_Company": "margin_company",
    "Margin_Competitor_A": "margin_competitor_a",
    "Margin_Competitor_B": "margin_competitor_b",
    "Margin_Competitor_C": "margin_competitor_c",
    "Margin_Competitor_D": "margin_competitor_d",
    "Margin_Competitor_E": "margin_competitor_e",
    "Margin_Others": "margin_others"
}

# --- UTILITY FUNCTIONS ---

def is_company_proposal_empty(value):
    """
    Check if a company proposal value should be treated as null/empty for calculations.
    This includes cases where the value is 0.00000 or similar zero values,
    which indicates the fields are not filled, or when the value is "NAP".
    Note: "NAP" is preserved in the database but treated as empty for calculations.
    """
    if value is None or value == "" or value == "-":
        return True
    
    # Convert to string for checking
    str_value = str(value).strip()
    
    # Check for empty string variations and NAP
    if str_value in ["", "-", "NAP", "None"]:
        return True
    
    # Check for zero values (0, 0.0, 0.00000, etc.)
    try:
        numeric_value = float(str_value.replace(',', '.'))
        # If the value is exactly zero or very close to zero, treat as empty
        if abs(numeric_value) < 0.000001:  # Using small epsilon for floating point comparison
            return True
    except (ValueError, TypeError):
        # If it can't be converted to float, check if it's a special string
        pass
    
    return False

def normalize_date_format(date_str):
    """Convert various date formats to DD/MM/YYYY for database consistency"""
    if not date_str or date_str == "":
        return None
    
    # Try various formats to parse the date
    date_formats = [
        "%Y-%m-%d",     # YYYY-MM-DD
        "%d/%m/%Y",     # DD/MM/YYYY
        "%d-%m-%Y",     # DD-MM-YYYY
        "%Y/%m/%d",     # YYYY/MM/DD
        "%d.%m.%Y",     # DD.MM.YYYY
    ]
    
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            # Always return in DD/MM/YYYY format
            return date_obj.strftime("%d/%m/%Y")
        except ValueError:
            continue
    
    # If no format matched, try to extract parts manually for DD/MM/YYYY format
    try:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
            # Already in DD/MM/YYYY format
            return date_str
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            # YYYY-MM-DD format
            year, month, day = date_str.split('-')
            return f"{day}/{month}/{year}"
    except Exception:
        pass
    
    return None  # Return None if no format matched

def normalize_number(value):
    """Normalize number values by converting strings with commas to proper floats"""
    if value is None or value == "":
        return None
        
    if isinstance(value, (int, float)):
        return value
        
    if isinstance(value, str):
        # Remove any non-breaking spaces or other whitespace
        value = value.strip()
        
        # Skip if it's a special value
        if value in ['-', 'NAP', 'None']:
            return None
            
        # Check for "k=" prefix and extract the numeric part if present
        k_match = re.search(r'k\s*=\s*([-+]?\d*[.,]?\d+)', value, re.IGNORECASE)
        if k_match:
            value = k_match.group(1)
        
        # First try with comma as decimal separator (European format)
        if ',' in value and '.' not in value:
            try:
                return float(value.replace(',', '.'))
            except ValueError:
                pass
        
        # Try with standard decimal point
        try:
            return float(value)
        except ValueError:
            # Try to extract numeric part if there's text mixed with numbers
            match = re.search(r'[-+]?\d*\.?\d+', value)
            if match:
                try:
                    return float(match.group())
                except ValueError:
                    pass
                    
            # If there's a comma, try to interpret it as a decimal separator
            match = re.search(r'[-+]?\d*,?\d+', value)
            if match:
                try:
                    return float(match.group().replace(',', '.'))
                except ValueError:
                    pass
            
    return None

def clean_numeric_value(value):
    """Convert various string formats to numeric values."""
    if pd.isna(value) or value in ['-', 'NAP', '', None]:
        return None
    
    if isinstance(value, str):
        # Skip values that have seasonal indicators
        if re.search(r'1\.º\s*Trim|1º\s*Trim', value):
            return None
            
        # Handle commas as decimal separators
        value = value.replace(',', '.')
        
        # Extract numeric part if it contains a number
        match = re.search(r'[-+]?\d*\.\d+|\d+', value)
        if match:
            return float(match.group())
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def extract_k_value(value):
    """Extract k= value from string and convert to float."""
    if pd.isna(value) or not isinstance(value, str):
        return None
    
    # Skip values that have seasonal indicators
    if re.search(r'1\.º\s*Trim|1º\s*Trim', value):
        return None
        
    # Try to extract k value with various formats: k=X,XXX, k= X,XXX, k=X.XXX, etc.
    k_match = re.search(r'k\s*=\s*([-+]?\d*[.,]?\d+)', value, re.IGNORECASE)
    if k_match:
        k_value = k_match.group(1).replace(',', '.')
        try:
            return float(k_value)
        except (ValueError, TypeError):
            return None
            
    # If the value itself is just a number and doesn't have k= prefix but should be treated as a k value
    # in certain indexing contexts, this logic allows the caller to interpret it accordingly
    if re.match(r'^[-+]?\d*[.,]?\d+$', value.strip()):
        try:
            return float(value.replace(',', '.'))
        except (ValueError, TypeError):
            return None
            
    return None

def format_float_with_commas(value, decimals=3):
    """Format a float value with comma as decimal separator."""
    if value is None:
        return None
        
    try:
        # Convert to float first to ensure we're working with a number
        float_value = float(value)
        
        # Format with specified decimal places and replace decimal point with comma
        formatted = f"{float_value:.{decimals}f}".replace('.', ',')
        
        # Remove trailing zeros after decimal separator, but keep at least one decimal place
        if decimals > 0:
            parts = formatted.split(',')
            if len(parts) > 1:
                # Remove trailing zeros but keep at least one decimal
                decimal_part = parts[1].rstrip('0')
                if not decimal_part:
                    decimal_part = '0'
                formatted = f"{parts[0]},{decimal_part}"
                
        return formatted
    except (ValueError, TypeError):
        return None

def generate_participating_companies(data):
    """Generate a list of companies that submitted proposals."""
    companies = []
    
    # Define generic competitor columns
    competitor_columns = {
        "PROPOSTA_COMPETITOR_A": "Competitor A",
        "PROPOSTA_COMPETITOR_B": "Competitor B", 
        "PROPOSTA_COMPETITOR_C": "Competitor C",
        "PROPOSTA_COMPETITOR_D": "Competitor D",
        "PROPOSTA_COMPETITOR_E": "Competitor E"
    }
    
    # Check each competitor proposal field
    for column, company in competitor_columns.items():
        if column in data and data[column]:
            value = str(data[column])
            # Consider a company as having a proposal if the value is not empty, 'NAP', or '-'
            if value not in ['-', '', 'NAP', 'None', None]:
                # Extra check for meaningful values
                k_value = extract_k_value(value)
                if k_value is not None or re.search(r'[0-9]', value):
                    companies.append(company)
    
    # Check others field
    if "proposta_concorrentes_outros" in data and data["proposta_concorrentes_outros"]:
        value = str(data["proposta_concorrentes_outros"])
        if value not in ['-', '', 'NAP', 'None', None]:
            if extract_k_value(value) is not None or re.search(r'[0-9]', value):
                companies.append("Others")
    
    # Add main company if they have a proposal
    if "propostaCompanyPE" in data and data["propostaCompanyPE"]:
        if not is_company_proposal_empty(data["propostaCompanyPE"]):
            value = str(data["propostaCompanyPE"])
            if extract_k_value(value) is not None or re.search(r'[0-9]', value):
                companies.append("Main Company")
    elif "proposto_company_k" in data and data["proposto_company_k"]:
        # Also check k value for main company in case PE is not set but k is
        if not is_company_proposal_empty(data["proposto_company_k"]):
            value = str(data["proposto_company_k"])
            if extract_k_value(value) is not None or re.search(r'[0-9]', value):
                companies.append("Main Company")
    
    return ", ".join(companies) if companies else None

def process_company_proposal(data):
    """
    Process main company proposal data to create the Company_Proposal field
    based on the indexing type and values.
    """
    indexante_tipo = data.get("proposto_company_indexante_tipo")
    
    if not indexante_tipo or indexante_tipo == "":
        return data
    
    # Handle different indexing types
    if indexante_tipo == "Fixo":
        # For fixed price type, use propostaCompanyPE * 1000 as the Company_Proposal
        company_pe = data.get("propostaCompanyPE")
        if company_pe and not is_company_proposal_empty(company_pe):
            company_pe_float = normalize_number(company_pe)
            if company_pe_float is not None:
                # Company_Proposal for fixed price should be propostaCompanyPE * 1000
                data["Company_Proposal"] = format_float_with_commas(company_pe_float * 1000, 3)
        else:
            # If Company PE is empty/zero, set Company_Proposal to None
            data["Company_Proposal"] = None
    elif indexante_tipo in ["Index Type A", "Index Type B"]:  # Generic indexing types
        # For indexed pricing, use k value for proposals
        k_value = data.get("proposto_company_k")
        if k_value and not is_company_proposal_empty(k_value):
            k_float = normalize_number(k_value)
            if k_float is not None:
                # Format as "k=X,XXX" with k value (multiplied by 1000 to convert to €/MWh)
                formatted_k = format_float_with_commas(k_float * 1000, 3)
                data["Company_Proposal"] = f"k={formatted_k}"
                
                # IMPORTANT: DO NOT modify propostaCompanyPE - keep original form value
                # Per requirements, propostaCompanyPE should remain as entered by user
        else:
            # If k value is empty/zero, set Company_Proposal to None
            data["Company_Proposal"] = None
    elif indexante_tipo == "Other":
        # For other indexing types, use PE directly if available
        pe_value = data.get("propostaCompanyPE")
        if pe_value and not is_company_proposal_empty(pe_value):
            pe_float = normalize_number(pe_value)
            if pe_float is not None:
                data["Company_Proposal"] = str(pe_float)
        else:
            # If PE value is empty/zero, set Company_Proposal to None
            data["Company_Proposal"] = None
    
    return data

def calculate_margins(data):
    """
    Calculate margin values for all companies based on their proposals
    and the fixed price.
    """
    # Process main company margin
    # Get company proposal from various possible sources
    indexante_tipo = data.get("proposto_company_indexante_tipo")
    company_proposal = None
    
    # For indexed types, use proposto_company_k directly
    if indexante_tipo in ["Index Type A", "Index Type B"]:
        if "proposto_company_k" in data and data["proposto_company_k"] is not None and not is_company_proposal_empty(data["proposto_company_k"]):
            # For these special types, the k value directly determines the margin
            k_value = normalize_number(data["proposto_company_k"])
            if k_value is not None:
                # For k values, the margin is the k value * 1000 (to convert to €/MWh)
                margin = k_value * 1000
                data["Margin_Company"] = format_float_with_commas(margin, 3)
        else:
            # If k value is empty/zero, set margin to None
            data["Margin_Company"] = None
    elif indexante_tipo == "Fixo":
        # For Fixed type, margin is propostaCompanyPE - fixed_price
        company_pe = None
        if "propostaCompanyPE" in data and data["propostaCompanyPE"] and not is_company_proposal_empty(data["propostaCompanyPE"]):
            company_pe = normalize_number(data["propostaCompanyPE"])
            
        fixed_price = normalize_number(data.get("fixed_price"))
        
        # Only calculate if we have both values
        if company_pe is not None and fixed_price is not None:
            # Convert PE to €/MWh if needed (usually in €/kWh)
            if company_pe < 1:  # Assuming small values are in €/kWh
                company_pe_mwh = company_pe * 1000
            else:
                company_pe_mwh = company_pe
            
            # Calculate margin as simple difference (not absolute)
            margin = company_pe_mwh - fixed_price
            data["Margin_Company"] = format_float_with_commas(margin, 3)
        else:
            # If Company PE is empty/zero or fixed price is missing, set margin to None
            data["Margin_Company"] = None
    else:
        # For other types, use the original logic with propostaCompanyPE
        # First try to use the direct PE value if it exists
        if "propostaCompanyPE" in data and data["propostaCompanyPE"] and not is_company_proposal_empty(data["propostaCompanyPE"]):
            company_proposal = data["propostaCompanyPE"]
        # Next try the already processed Company_Proposal value
        elif "Company_Proposal" in data and data["Company_Proposal"]:
            company_proposal = data["Company_Proposal"]
        
        if company_proposal:
            k_value = extract_k_value(str(company_proposal))
            
            if k_value is not None:
                # For k= values, use them directly (multiply by 1000 for MWh)
                margin = k_value * 1000
                data["Margin_Company"] = format_float_with_commas(margin, 3)
            else:
                # Regular company price for other types
                company_price = clean_numeric_value(str(company_proposal))
                
                if company_price is not None:
                    # For non-Fixed types, PE is already the margin (usually the k value)
                    if company_price < 1:  # Assuming small values are in €/kWh
                        margin = company_price * 1000  # Convert to €/MWh
                    else:
                        margin = company_price  # Already in €/MWh
                    data["Margin_Company"] = format_float_with_commas(margin, 2)

    # Process competitor margins
    competitor_cols = [
        ("PROPOSTA_COMPETITOR_A", "Margin_Competitor_A"),
        ("PROPOSTA_COMPETITOR_B", "Margin_Competitor_B"),
        ("PROPOSTA_COMPETITOR_C", "Margin_Competitor_C"),
        ("PROPOSTA_COMPETITOR_D", "Margin_Competitor_D"),
        ("PROPOSTA_COMPETITOR_E", "Margin_Competitor_E")
    ]
    
    for col, margin_col in competitor_cols:
        if col in data and data[col] and data[col] != "NAP":
            k_value = extract_k_value(data[col])
            
            if k_value is not None:
                # For k= values, multiply by 1000 to convert to €/MWh
                margin = k_value * 1000
                data[margin_col] = format_float_with_commas(margin, 3)
            else:
                # Regular competitor price
                comp_price = clean_numeric_value(data[col])
                
                if comp_price is not None:
                    # For Fixed type with fixed_price available
                    if indexante_tipo == "Fixo" and data.get("fixed_price"):
                        fixed_price = normalize_number(data.get("fixed_price"))
                        if fixed_price is not None:
                            # Convert from €/kWh to €/MWh
                            comp_price_mwh = comp_price * 1000
                            
                            # Calculate margin as simple difference (not absolute)
                            margin = comp_price_mwh - fixed_price
                            data[margin_col] = format_float_with_commas(margin, 3)
                    else:
                        # For non-Fixed types, the price itself is the margin
                        margin = comp_price * 1000  # Convert to €/MWh
                        data[margin_col] = format_float_with_commas(margin, 2)
    
    return data

def calculate_profit_margin(data):
    """
    Calculate profit margin (€/MWh) and real profit based on fixed price,
    winner proposal, and consumption.
    """
    winner = data.get("vencedor")
    
    if not winner:
        return data
    
    # Determine winner proposal based on who won
    indexante_tipo = data.get("proposto_company_indexante_tipo")
    is_indexed_type = indexante_tipo in ["Index Type A", "Index Type B"]
    
    if winner == "Main Company":
        # Check if company proposal is empty/zero first
        company_pe = data.get("propostaCompanyPE")
        company_k = data.get("proposto_company_k")
        
        # If both PE and k values are empty/zero, set all related fields to None
        if (is_company_proposal_empty(company_pe) and is_company_proposal_empty(company_k)):
            data["proposta_vencedor"] = None
            data["Profit_Margin"] = None
            data["Real_profit"] = None
        # For main company, calculate propostaVencedor based on indexante_tipo
        elif indexante_tipo == "Fixo":
            # For Fixed type, use propostaCompanyPE * 1000
            pe_value = normalize_number(data.get("propostaCompanyPE"))
            if pe_value is not None and not is_company_proposal_empty(data.get("propostaCompanyPE")):
                # Calculate value in correct units (€/MWh)
                company_proposal_value = pe_value * 1000
                # Format with comma as decimal separator
                data["proposta_vencedor"] = format_float_with_commas(company_proposal_value, 3)
                
                # For Fixed type, profit margin is propostaVencedor - fixed_price
                fixed_price = normalize_number(data.get("fixed_price"))
                if fixed_price is not None:
                    # Calculate profit margin in €/MWh
                    profit_margin = company_proposal_value - fixed_price
                    data["Profit_Margin"] = format_float_with_commas(profit_margin, 3)
                    
                    # Calculate real profit if consumption is available
                    consumo = normalize_number(data.get("consumoTotalKWh"))
                    if consumo is not None:
                        # Convert consumo from kWh to MWh (divide by 1000)
                        real_profit = (consumo / 1000) * profit_margin
                        data["Real_profit"] = format_float_with_commas(real_profit, 2)
        elif is_indexed_type:
            # For indexed types, use proposto_company_k value directly (not multiplied by 1000)
            k_value = normalize_number(data.get("proposto_company_k"))
            if k_value is not None and not is_company_proposal_empty(data.get("proposto_company_k")):
                # Calculate value in correct units (€/MWh)
                company_proposal_value = k_value * 1000
                # Format with comma as decimal separator and add k= prefix
                formatted_value = format_float_with_commas(company_proposal_value, 3)
                data["proposta_vencedor"] = f"k={formatted_value}"
                
                # For indexed types, profit margin is the k value directly (in €/MWh)
                data["Profit_Margin"] = formatted_value
                
                # Calculate real profit if consumption is available
                consumo = normalize_number(data.get("consumoTotalKWh"))
                if consumo is not None:
                    # Convert consumo from kWh to MWh (divide by 1000)
                    real_profit = (consumo / 1000) * company_proposal_value
                    data["Real_profit"] = format_float_with_commas(real_profit, 2)
        else:
            # Fallback to propostaCompanyPE if available
            if "propostaCompanyPE" in data and data["propostaCompanyPE"] and not is_company_proposal_empty(data.get("propostaCompanyPE")):
                pe_value = normalize_number(data.get("propostaCompanyPE"))
                if pe_value is not None:
                    # Convert to €/MWh
                    company_proposal_value = pe_value * 1000
                    data["proposta_vencedor"] = format_float_with_commas(company_proposal_value, 3)
                    
                    # For other types, use the value directly as profit margin
                    data["Profit_Margin"] = format_float_with_commas(company_proposal_value, 3)
                    
                    # Calculate real profit if consumption is available
                    consumo = normalize_number(data.get("consumoTotalKWh"))
                    if consumo is not None:
                        # Convert consumo from kWh to MWh (divide by 1000)
                        real_profit = (consumo / 1000) * company_proposal_value
                        data["Real_profit"] = format_float_with_commas(real_profit, 2)
    
    # Handle competitor winners
    elif winner in ["Competitor A", "Competitor B", "Competitor C", "Competitor D", "Competitor E"]:
        # Map winner name to column name
        winner_mapping = {
            "Competitor A": "PROPOSTA_COMPETITOR_A",
            "Competitor B": "PROPOSTA_COMPETITOR_B", 
            "Competitor C": "PROPOSTA_COMPETITOR_C",
            "Competitor D": "PROPOSTA_COMPETITOR_D",
            "Competitor E": "PROPOSTA_COMPETITOR_E"
        }
        
        col_name = winner_mapping.get(winner)
        if col_name and col_name in data and data[col_name] and data[col_name] != "NAP":
            original_value = data[col_name]
            
            # Check if this is a k= value proposal
            k_value = extract_k_value(str(original_value))
            
            if k_value is not None and is_indexed_type:
                # For k= values with indexed types, format with k= prefix
                comp_value_mwh = k_value * 1000
                formatted_value = format_float_with_commas(comp_value_mwh, 3)
                data["proposta_vencedor"] = f"k={formatted_value}"
                
                # For indexed types, profit margin is just the formatted value (without k= prefix)
                data["Profit_Margin"] = formatted_value
                
                # Calculate real profit if consumption is available
                consumo = normalize_number(data.get("consumoTotalKWh"))
                if consumo is not None:
                    # Convert consumo from kWh to MWh (divide by 1000)
                    real_profit = (consumo / 1000) * comp_value_mwh
                    data["Real_profit"] = format_float_with_commas(real_profit, 2)
            else:
                # Get the competitor proposal value
                comp_value = normalize_number(original_value)
                if comp_value is not None:
                    # Convert to €/MWh
                    comp_value_mwh = comp_value * 1000
                    
                    # Set propostaVencedor with proper formatting
                    # For indexed types, add k= prefix if not already present
                    if is_indexed_type and not str(original_value).lower().startswith("k="):
                        formatted_value = format_float_with_commas(comp_value_mwh, 3)
                        data["proposta_vencedor"] = f"k={formatted_value}"
                        # For indexed types, profit margin is just the formatted value (without k= prefix)
                        data["Profit_Margin"] = formatted_value
                    else:
                        data["proposta_vencedor"] = format_float_with_commas(comp_value_mwh, 3)
                        
                        # Calculate profit margin based on indexing type
                        if indexante_tipo == "Fixo" and data.get("fixed_price"):
                            # For Fixed type with fixed_price, calculate profit margin as propostaVencedor - fixed_price
                            fixed_price = normalize_number(data.get("fixed_price"))
                            if fixed_price is not None:
                                # Calculate profit margin
                                profit_margin = comp_value_mwh - fixed_price
                                data["Profit_Margin"] = format_float_with_commas(profit_margin, 3)
                                
                                # Calculate real profit if consumption is available
                                consumo = normalize_number(data.get("consumoTotalKWh"))
                                if consumo is not None:
                                    # Convert consumo from kWh to MWh (divide by 1000)
                                    real_profit = (consumo / 1000) * profit_margin
                                    data["Real_profit"] = format_float_with_commas(real_profit, 2)
                        else:
                            # For non-Fixed types without k=, profit margin is the same as propostaVencedor
                            data["Profit_Margin"] = data["proposta_vencedor"]
                            
                            # Calculate real profit if consumption is available
                            consumo = normalize_number(data.get("consumoTotalKWh"))
                            if consumo is not None:
                                # Convert consumo from kWh to MWh (divide by 1000)
                                real_profit = (consumo / 1000) * comp_value_mwh
                                data["Real_profit"] = format_float_with_commas(real_profit, 2)
    
    elif winner == "Others" and data.get("proposta_concorrentes_outros"):
        # For "Others" winner, use the proposta_concorrentes_outros field
        original_value = data.get("proposta_concorrentes_outros")
        
        # Check if this is a k= value proposal
        k_value = extract_k_value(str(original_value)) if original_value else None
        
        if k_value is not None and is_indexed_type:
            # For k= values with indexed types, format with k= prefix
            outro_value_mwh = k_value * 1000
            formatted_value = format_float_with_commas(outro_value_mwh, 3)
            data["proposta_vencedor"] = f"k={formatted_value}"
            
            # For indexed types, profit margin is just the formatted value (without k= prefix)
            data["Profit_Margin"] = formatted_value
            
            # Calculate real profit if consumption is available
            consumo = normalize_number(data.get("consumoTotalKWh"))
            if consumo is not None:
                # Convert consumo from kWh to MWh (divide by 1000)
                real_profit = (consumo / 1000) * outro_value_mwh
                data["Real_profit"] = format_float_with_commas(real_profit, 2)
        else:
            # Regular numeric value
            outro_value = normalize_number(original_value)
            if outro_value is not None:
                # Convert to €/MWh
                outro_value_mwh = outro_value * 1000
                
                # Set propostaVencedor with proper formatting
                # For indexed types, add k= prefix if not already present
                if is_indexed_type and not str(original_value).lower().startswith("k="):
                    formatted_value = format_float_with_commas(outro_value_mwh, 3)
                    data["proposta_vencedor"] = f"k={formatted_value}"
                    # For indexed types, profit margin is just the formatted value (without k= prefix)
                    data["Profit_Margin"] = formatted_value
                else:
                    data["proposta_vencedor"] = format_float_with_commas(outro_value_mwh, 3)
                    
                    # Calculate profit margin based on indexing type
                    if indexante_tipo == "Fixo" and data.get("fixed_price"):
                        # For Fixed type, profit margin is propostaVencedor - fixed_price
                        fixed_price = normalize_number(data.get("fixed_price"))
                        if fixed_price is not None:
                            # Calculate profit margin
                            profit_margin = outro_value_mwh - fixed_price
                            data["Profit_Margin"] = format_float_with_commas(profit_margin, 3)
                            
                            # Calculate real profit if consumption is available
                            consumo = normalize_number(data.get("consumoTotalKWh"))
                            if consumo is not None:
                                # Convert consumo from kWh to MWh (divide by 1000)
                                real_profit = (consumo / 1000) * profit_margin
                                data["Real_profit"] = format_float_with_commas(real_profit, 2)
                    else:
                        # For non-Fixed types without k=, profit margin is the same as propostaVencedor
                        data["Profit_Margin"] = data["proposta_vencedor"]
                        
                        # Calculate real profit if consumption is available
                        consumo = normalize_number(data.get("consumoTotalKWh"))
                        if consumo is not None:
                            # Convert consumo from kWh to MWh (divide by 1000)
                            real_profit = (consumo / 1000) * outro_value_mwh
                            data["Real_profit"] = format_float_with_commas(real_profit, 2)
    
    return data

def clean_and_transform(raw_data):
    """
    Process and transform the raw contest data before database insertion.
    This function normalizes data types and calculates derived fields.
    """
    # Log the raw data
    logger.info(f"Processing raw data: {json.dumps(raw_data, default=str)}")
    
    processed = raw_data.copy()
    
    # Keep original values for competitor proposal fields
    competitor_cols = [
        "PROPOSTA_COMPETITOR_A",
        "PROPOSTA_COMPETITOR_B",
        "PROPOSTA_COMPETITOR_C", 
        "PROPOSTA_COMPETITOR_D",
        "PROPOSTA_COMPETITOR_E"
    ]
    
    # Create a backup of the original competitor values
    original_competitor_values = {}
    for col in competitor_cols:
        if col in processed:
            original_competitor_values[col] = processed[col]
    
    # Set empty competitor fields to "NAP"
    for col in competitor_cols:
        if col in processed and (processed[col] is None or processed[col] == "" or processed[col] == "-"):
            processed[col] = "NAP"
    
    # Ensure concursoNum is defined - this is a primary key
    if not processed.get("concursoNum"):
        raise ValueError("concursoNum is required and cannot be empty")
    
    # Normalize dates
    date_fields = ["dataPublicacao", "prazoEntregaData"]
    for field in date_fields:
        if field in processed and processed[field]:
            processed[field] = normalize_date_format(processed[field])
    
    # Ensure dataPublicacao is defined - needed for source_year
    if not processed.get("dataPublicacao"):
        logger.warning("dataPublicacao is required but missing - defaulting to current date")
        processed["dataPublicacao"] = datetime.now().strftime("%d/%m/%Y")
    
    # Extract source_year from dataPublicacao - this is a primary key
    # Try to extract year from dataPublicacao
    if "dataPublicacao" in processed and processed["dataPublicacao"]:
        # Extract year from date in DD/MM/YYYY format
        try:
            if isinstance(processed["dataPublicacao"], str):
                # Try to extract year from end if in format dd/mm/yyyy
                year_match = re.search(r'(\d{4})$', processed["dataPublicacao"])
                if year_match:
                    processed["source_year"] = year_match.group(1)
                else:
                    # Try to extract from middle if in format dd/mm/yyyy
                    year_match = re.search(r'/(\d{4})/', processed["dataPublicacao"])
                    if year_match:
                        processed["source_year"] = year_match.group(1)
                    else:
                        # Default to current year if no pattern matched
                        processed["source_year"] = str(datetime.now().year)
            else:
                processed["source_year"] = str(datetime.now().year)
        except Exception as e:
            logger.warning(f"Error extracting source_year: {str(e)}")
            processed["source_year"] = str(datetime.now().year)
    else:
        # Default to current year if no publication date
        processed["source_year"] = str(datetime.now().year)
    
    # Normalize numeric values
    numeric_fields = [
        "consumoTotalKWh", 
        "valorContrato", 
        "proposto_company_indexante_cotacao", 
        "proposto_company_k", 
        "fixed_price",
        "proposta_concorrentes_outros"
    ]
    
    # Only normalize non-competitor fields
    for field in numeric_fields:
        if field in processed and processed[field]:
            processed[field] = normalize_number(processed[field])
    
    # Special handling for propostaCompanyPE - preserve "NAP" value but normalize numbers
    if "propostaCompanyPE" in processed and processed["propostaCompanyPE"]:
        if str(processed["propostaCompanyPE"]).upper() == "NAP":
            # Keep NAP as string - this is the user's explicit choice
            processed["propostaCompanyPE"] = "NAP"
        else:
            # Normalize as number for other values
            processed["propostaCompanyPE"] = normalize_number(processed["propostaCompanyPE"])
    
    # Ensure referencia field is included
    if "referencia" not in processed:
        processed["referencia"] = ""
    
    # Check if this is an indexed type tender
    indexante_tipo = processed.get("proposto_company_indexante_tipo", "")
    is_indexed_type = indexante_tipo in ["Index Type A", "Index Type B"]
    
    # If this is indexed type, ensure competitor proposals have "k=" prefix if they don't already
    if is_indexed_type:
        for col in competitor_cols:
            if col in processed and processed[col] and processed[col] != "NAP":
                val = str(processed[col])
                # Only add k= prefix if it doesn't already have one and looks like a number
                if not val.lower().startswith("k=") and not "k=" in val.lower():
                    # If it's just a number without k=, add the prefix
                    if re.match(r'^-?\d*[.,]?\d+$', val.strip()):
                        processed[col] = f"k={val}"
        
        # Also ensure proposta_concorrentes_outros has k= prefix if needed
        if "proposta_concorrentes_outros" in processed and processed["proposta_concorrentes_outros"]:
            val = str(processed["proposta_concorrentes_outros"])
            if not val.lower().startswith("k=") and not "k=" in val.lower():
                if re.match(r'^-?\d*[.,]?\d+$', val.strip()):
                    processed["proposta_concorrentes_outros"] = f"k={val}"
    
    # Generate the list of companies with proposals
    processed["empresasComPropostas"] = generate_participating_companies(processed)
    
    # Process company proposal data
    processed = process_company_proposal(processed)
    
    # Calculate margins - temporarily normalize competitor values for calculations
    temp_processed = processed.copy()
    for col in competitor_cols:
        if col in temp_processed and temp_processed[col] and temp_processed[col] != "NAP":
            # For calculation purposes, extract numeric value but preserve original format
            k_value = extract_k_value(temp_processed[col])
            if k_value is not None:
                temp_processed[col] = k_value
            else:
                temp_processed[col] = normalize_number(temp_processed[col])
    
    # Calculate margins using temporary values
    temp_processed = calculate_margins(temp_processed)
    
    # Copy only the margin values back to the processed data
    for col_name, margin_name in [
        ("PROPOSTA_COMPETITOR_A", "Margin_Competitor_A"),
        ("PROPOSTA_COMPETITOR_B", "Margin_Competitor_B"),
        ("PROPOSTA_COMPETITOR_C", "Margin_Competitor_C"),
        ("PROPOSTA_COMPETITOR_D", "Margin_Competitor_D"),
        ("PROPOSTA_COMPETITOR_E", "Margin_Competitor_E")
    ]:
        if margin_name in temp_processed:
            processed[margin_name] = temp_processed[margin_name]
    
    # Also copy company margin
    if "Margin_Company" in temp_processed:
        processed["Margin_Company"] = temp_processed["Margin_Company"]
    
    # Calculate profit margin and real profit
    processed = calculate_profit_margin(processed)
    
    # For indexed types, we need to keep competitor values with k= prefix in the final data
    # Only restore the original values for non-indexed cases or if we didn't have to add a prefix
    if not is_indexed_type:
        # Restore original values for competitor proposal fields for non-indexed types
        for col, val in original_competitor_values.items():
            if val is not None and val != "":  # Only restore if there was an original value
                processed[col] = val
            elif val == "" or val is None or val == "-":
                # Ensure empty competitor values are set to "NAP"
                processed[col] = "NAP"
    else:
        # For indexed types, only restore empty values to "NAP"
        for col, val in original_competitor_values.items():
            if val == "" or val is None or val == "-":
                processed[col] = "NAP"
    
    # Add timestamp field
    processed["created_at"] = datetime.now().isoformat()
    processed["processed_by_function"] = True
    
    # Apply column mapping to ensure we're using the correct column names
    mapped_data = {}
    for key, value in processed.items():
        # Try to map the column name using COLUMN_MAPPING
        if key in COLUMN_MAPPING:
            mapped_key = COLUMN_MAPPING[key]
        else:
            # If no mapping exists, convert to lowercase as fallback
            mapped_key = key.lower()
        
        mapped_data[mapped_key] = value
    
    # Log the processed data with mapped column names
    logger.info(f"Processed data with mapped columns: {json.dumps(mapped_data, default=str)}")
    
    return mapped_data

# Example usage function
def process_example_data():
    """
    Example function demonstrating how to use the contest processing logic.
    """
    # Example raw data structure
    sample_data = {
        "concursoNum": "2024-001",
        "nome": "Sample Contest",
        "dataPublicacao": "15/03/2024",
        "prazoEntregaData": "30/03/2024",
        "consumoTotalKWh": 1000000,
        "valorContrato": 45000,
        "proposto_company_indexante_tipo": "Index Type A",
        "proposto_company_k": 2.5,
        "propostaCompanyPE": 0.045,
        "fixed_price": 42.5,
        "PROPOSTA_COMPETITOR_A": "k=3.2",
        "PROPOSTA_COMPETITOR_B": "0.047",
        "PROPOSTA_COMPETITOR_C": "NAP",
        "PROPOSTA_COMPETITOR_D": "k=2.8", 
        "PROPOSTA_COMPETITOR_E": "NAP",
        "proposta_concorrentes_outros": "",
        "vencedor": "Main Company",
        "referencia": "REF-2024-001"
    }
    
    # Process the data
    processed_data = clean_and_transform(sample_data)
    
    return processed_data

if __name__ == "__main__":
    # Example usage
    result = process_example_data()
    print("Processed contest data:", json.dumps(result, indent=2, default=str)) 