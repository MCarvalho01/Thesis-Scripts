# Thesis-Scripts
Scripts for jury analysis while reading:

process_contracts.py
This script does the data extraction from the portal base.gov.

process_contracts_details.py
This script does the data extraction from the portal base.gov details page.
 
vortal_extract.py
This script extracts the documents from the Vortal platform URLs available in the csv.

anoGov_extract.py
This script extracts the documents from the AnoGov platform URLs available in the csv.

contract_down.py
This script extracts the documents directly form the URLs available in the csv.

ext_unstructured.py
This script extracts the text of the documents.

analyze_contracts.py
This script does the analysis of the extracted text to find the gas Index reference.

gas_tender_processor
This script standerdizes the raw data and cleans any wrong inputted values from the Excel. Also does a proposal comparison analysis and calculates key metrics.

data_transform.py
This script processes and analyzes the tenders data, standardizes the data and calculates the margins for the different specifications of the companies. Is the script that uses the AI model for the gas calculations.

gas_price_calculator.py
This script creates the AI Assitant that will calculate the gas price using the calculaltion training provided in the prompt.

main.py
Script for the Google Cloud Function that does the data transformation for the contests submitted via Form.
