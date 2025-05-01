#!/usr/bin/env python3
"""
Script to generate sample ownership Excel file for testing.
"""
import pandas as pd
import os

def create_sample_ownership_file():
    """
    Create a sample ownership Excel file from the text data.
    """
    # First 3 rows are metadata
    metadata = [
        ["View Name:", "NORI Ownership", "", "", "", "", "", ""],
        ["Date Range:", "05-01-2025 to 05-01-2025", "", "", "", "", "", ""],
        ["Portfolio:", "All clients", "", "", "", "", "", ""]
    ]
    
    # Convert metadata to DataFrame
    metadata_df = pd.DataFrame(metadata)
    
    # Create the header row (4th row in the Excel file)
    headers = ["Client", "Entity ID", "Holding Account Number", "Portfolio", 
               "Group ID", "Data Inception Date", "% Ownership", "Grouping Attribute Name"]
    
    # Sample data for a few entities
    data = [
        ["The Linden East II Trust (Abigail Wexner)", "27779629", "", "The Linden East II Trust", 
         "", "Dec 24, 2022", "", "Client"],
        ["Wexner Grandchildren Trusts", "", "", "The Linden East II Trust", 
         "1776240", "Dec 24, 2022", "", "Group"],
        ["Linden East II Trust Custom Muni Bond Account", "18588203", "G36471003", "The Linden East II Trust", 
         "", "Dec 24, 2022", "100.00%", "Holding Account"],
        ["Linden East II Trust Edgewood Large Cap Growth Account", "18588276", "G22603007", "The Linden East II Trust", 
         "", "Dec 24, 2022", "100.00%", "Holding Account"],
        ["Linden East II Trust JP Morgan Customized Municipal Bond Account PTC", "134313231", "T20455009", "The Linden East II Trust", 
         "", "Apr 25, 2025", "100.00%", "Holding Account"],
        ["18 Sole LLC", "21450199", "", "18 Sole LLC", 
         "", "Mar 1, 2023", "", "Client"],
        ["18 Sole LLC Reporting", "", "", "18 Sole LLC", 
         "1801747", "Mar 1, 2023", "", "Group"],
        ["18 Sole HarbourVest Access - Dover Street XI LLC", "22584854", "", "18 Sole LLC", 
         "", "Mar 1, 2023", "100.00%", "Holding Account"],
        ["Aaron Abrahms", "4577258", "", "", 
         "", "Jun 29, 2018", "", "Client"],
        ["Aaron Abrahms Family ALL Assets", "", "", "", 
         "1520174", "Jun 29, 2018", "", "Group"],
        ["Aaron Abrahms Bene Dec'd IRA", "14400096", "663039452", "Abrahms, Aaron Reporting", 
         "", "Mar 24, 2022", "100.00%", "Holding Account"],
        ["Aaron Abrahms Outside Assets", "5371116", "", "Abrahms, Aaron Family 100%, Abrahms, Aaron Illiquid 100%", 
         "", "Jun 29, 2018", "100.00%", "Holding Account"]
    ]
    
    # Convert to DataFrame
    data_df = pd.DataFrame(data, columns=headers)
    
    # Create Excel file using ExcelWriter
    output_path = os.path.join(os.path.dirname(__file__), "sample_ownership.xlsx")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write metadata (first 3 rows without headers)
        metadata_df.to_excel(writer, sheet_name='Sheet1', header=False, index=False)
        
        # Write data starting from row 4
        data_df.to_excel(writer, sheet_name='Sheet1', header=True, index=False, startrow=4)
        
    print(f"Sample ownership file created at: {output_path}")

if __name__ == "__main__":
    create_sample_ownership_file()