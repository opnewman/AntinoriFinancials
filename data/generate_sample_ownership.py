#!/usr/bin/env python3
"""
Script to generate sample ownership Excel file for testing.
"""

import os
import pandas as pd
from datetime import datetime

# Path to the text file
INPUT_FILE = "attached_assets/noriownershipexample.txt"
OUTPUT_FILE = "data/sample_ownership.xlsx"

def create_sample_ownership_file():
    """
    Create a sample ownership Excel file from the text data.
    """
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return False
    
    # Read the text file
    with open(INPUT_FILE, 'r') as f:
        lines = f.readlines()
    
    # Extract metadata (first 3 lines)
    metadata = []
    for i in range(3):
        if i < len(lines):
            metadata.append(lines[i].strip())
    
    # Read the column headers (4th line)
    headers = []
    if len(lines) > 3:
        headers = [header.strip() for header in lines[3].strip().split('\t')]
    
    # Read the data rows (5th line onwards)
    data = []
    for i in range(4, len(lines)):
        row_values = [value.strip() for value in lines[i].strip().split('\t')]
        if len(row_values) == len(headers):
            data.append(row_values)
        else:
            print(f"Warning: Row {i+1} has {len(row_values)} values, expected {len(headers)}")
            # Try to pad or truncate
            if len(row_values) < len(headers):
                row_values += [''] * (len(headers) - len(row_values))
            else:
                row_values = row_values[:len(headers)]
            data.append(row_values)
    
    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)
    
    # Create Excel writer
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Write metadata
        metadata_df = pd.DataFrame([[metadata[0], ''], [metadata[1], ''], [metadata[2], '']])
        metadata_df.to_excel(writer, index=False, header=False, sheet_name='Sheet1')
        
        # Write data
        df.to_excel(writer, index=False, startrow=3, sheet_name='Sheet1')
    
    print(f"Sample ownership file created: {OUTPUT_FILE}")
    print(f"Total rows: {len(data)}")
    return True

if __name__ == "__main__":
    create_sample_ownership_file()