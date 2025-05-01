# Sample Data Files for ANTINORI

This directory should contain the following Excel files for uploading to the ANTINORI system:

## 1. data_dump.xlsx

**Format:**
A spreadsheet containing financial positions with the following columns:
- position (str): Unique identifier for the position
- top_level_client (str): Client name
- holding_account (str): Account name
- holding_account_number (str): Account number
- portfolio (str): Portfolio name
- cusip (str): CUSIP identifier
- ticker_symbol (str): Stock ticker or identifier
- asset_class (str): Primary asset class
- second_level (str): Secondary classification
- third_level (str): Tertiary classification
- adv_classification (str): Advanced classification
- liquid_vs_illiquid (str): "Liquid" or "Illiquid"
- adjusted_value (float): Dollar value (will be encrypted)
- date (date): Position date

**Sample Data (first few rows):**
```
position,top_level_client,holding_account,holding_account_number,portfolio,cusip,ticker_symbol,asset_class,second_level,third_level,adv_classification,liquid_vs_illiquid,adjusted_value
EQUITY_001,Sprackman Family,Account1,ACC001,Penny Portfolio,CUSIP001,AAPL,Equity,US Markets,,Standard,Liquid,500000
EQUITY_002,Katz Family,Account2,ACC002,David Portfolio,CUSIP002,MSFT,Equity,Venture Capital,,Aggressive,Illiquid,200000
BOND_001,Sprackman Family,Account1,ACC001,Penny Portfolio,CUSIP003,TLT,Fixed Income,US Treasury,10-Year,Conservative,Liquid,300000
```

## 2. ownership.xlsx

**Format:**
A spreadsheet defining the ownership hierarchy with the following columns:
- holding_account (str): Account name
- holding_account_number (str): Account number
- top_level_client (str): Client name
- entity_id (str): Unique entity identifier
- portfolio (str): Portfolio name
- groups (str, optional): Grouping information

**Sample Data (first few rows):**
```
holding_account,holding_account_number,top_level_client,entity_id,portfolio,groups
Account1,ACC001,Sprackman Family,ENT001,Penny Portfolio,Family Trust
Account2,ACC002,Katz Family,ENT002,David Portfolio,Retirement
Account3,ACC003,Sprackman Family,ENT003,John Portfolio,Education
```

## 3. risk_stats.xlsx

**Format:**
A multi-tab spreadsheet containing risk statistics for different asset classes:

**Tab 1: Equity**
- position (str): Position identifier
- ticker_symbol (str): Stock ticker
- vol (float): Volatility (percentage)
- beta (float): Beta value

**Tab 2: Fixed Income**
- position (str): Position identifier
- ticker_symbol (str): Bond ticker
- vol (float): Volatility (percentage)
- duration (float): Duration (years)

**Tab 3: Alternatives**
- position (str): Position identifier
- ticker_symbol (str): Alternative asset ticker
- vol (float): Volatility (percentage)
- beta_to_gold (float): Beta to gold

**Sample Data:**
```
[Equity Tab]
position,ticker_symbol,vol,beta
EQUITY_001,AAPL,0.25,1.2
EQUITY_002,MSFT,0.20,1.1

[Fixed Income Tab]
position,ticker_symbol,vol,duration
BOND_001,TLT,0.15,5.2
BOND_002,AGG,0.10,4.1

[Alternatives Tab]
position,ticker_symbol,vol,beta_to_gold
ALT_001,GOLD_FUTURE_001,0.22,0.95
ALT_002,RE_FUND_001,0.18,0.25
```

## Important Notes

1. All Excel files should use the first row for column headers.
2. Dates should be in YYYY-MM-DD format.
3. Numerical values should not include currency symbols or commas.
4. The system will validate the data upon upload and report any errors.