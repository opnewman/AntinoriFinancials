"""
Test script for validating our bond pattern matching implementation.

This script tests various patterns commonly found in fixed income securities
to ensure our pattern recognition logic works correctly.
"""
import logging
import re
import datetime
from typing import List, Tuple, Dict, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Bond name patterns to test
TEST_BONDS = [
    # Format: (position_name, expected_rate, expected_year)
    ("US TREASURY 1.5% 2025", "1.5", "25"),
    ("US TREASURY 1.50% 02/15/2025", "1.50", "25"),
    ("VERIZON COMMUNICATIONS 5.5% 2047", "5.5", "47"),
    ("VERIZON COMMUNICATIONS 5.50% 03/16/2047", "5.50", "47"),
    ("FEDERAL HOME LOAN BANK 3.375% 2024 BOND", "3.375", "24"),
    ("FEDERAL HOME LOAN BANK 3.375% 06/12/2024", "3.375", "24"),
    ("FNMA 30YR 3.0", "3.0", None),  # No year in this pattern
    ("FHLMC 30YR UMBS SUPER", None, None),  # No rate or year
    ("AMAZON.COM INC 3.15% 01MAY2024", "3.15", "24"),
    ("AMAZON.COM INC 3.15% 05/01/2024", "3.15", "24"),
    ("WAL-MART STORES INC. 2.55% 04/11/2023", "2.55", "23"),
    ("WALMART INC 2.55% 04/11/2023", "2.55", "23"),
    ("APPLE 3.85% 05/04/2043", "3.85", "43"),
    ("Apple Inc. 3.85% 05/04/2043", "3.85", "43"),
    ("MICROSOFT CORP", None, None),  # No rate or year
    ("MICROSOFT CORP 2.4% 08/08/2026", "2.4", "26"),
    ("MSFT 2.4 08/08/26", "2.4", "26"),  # Abbreviated format
    ("American Express CO Note 1.65 % Due Nov 4, 2026", "1.65", "26"),
    ("AMERICAN EXPRESS CO 1.65% 11/04/2026", "1.65", "26")
]

# Regex patterns to extract rate and year
RATE_YEAR_REGEX = r'(\d+\.?\d*)\%.*?20(\d{2})'  # Basic pattern: X.X% ... 20XX
RATE_ONLY_REGEX = r'(\d+\.?\d*)\%'  # Just find rate: X.X%
YEAR_ONLY_REGEX = r'20(\d{2})'  # Just find year: 20XX
SPACED_RATE_REGEX = r'(\d+\.?\d*)\s+\%'  # Rate with space before %
ABBREVIATED_YEAR_REGEX = r'(\d{2})\/(\d{2})\/(\d{2})'  # MM/DD/YY format
DUE_DATE_REGEX = r'Due\s+.*?(\d{4})'  # "Due ... YYYY" format
SPACE_SEPARATED_RATE_REGEX = r'(\d+\.?\d*)\s'  # For cases like "2.4 08/08/26" 
MMDDYY_WITHOUT_SLASH_REGEX = r'(\d{2})(\d{2})(\d{2})'  # For formats like 080826

def extract_pattern(bond_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract interest rate and year from bond name using multiple patterns.
    
    Args:
        bond_name: The name of the bond
        
    Returns:
        Tuple of (interest_rate, year) where both could be None if not found
    """
    # Try to extract both rate and year with the main pattern
    match = re.search(RATE_YEAR_REGEX, bond_name)
    if match:
        rate, year = match.groups()
        return rate, year
    
    # If that doesn't work, try to extract rate and year separately
    rate_match = re.search(RATE_ONLY_REGEX, bond_name)
    year_match = re.search(YEAR_ONLY_REGEX, bond_name)
    
    rate = rate_match.group(1) if rate_match else None
    year = year_match.group(1) if year_match else None
    
    if rate is None:
        # Try spaced rate pattern
        spaced_match = re.search(SPACED_RATE_REGEX, bond_name)
        if spaced_match:
            rate = spaced_match.group(1)
            
        # Try space separated rate (like "2.4 08/08/26")
        if rate is None and ' ' in bond_name:
            words = bond_name.split()
            for word in words:
                if re.match(r'^\d+\.?\d*$', word):  # Just digits and possibly a decimal point
                    rate = word
                    break
    
    if year is None:
        # Try abbreviated year (MM/DD/YY) format
        abbr_match = re.search(ABBREVIATED_YEAR_REGEX, bond_name)
        if abbr_match:
            month, day, abbr_year = abbr_match.groups()
            year = abbr_year
        
        # Try "Due ... YYYY" format
        due_match = re.search(DUE_DATE_REGEX, bond_name)
        if due_match:
            full_year = due_match.group(1)
            year = full_year[-2:]  # Extract last 2 digits
            
        # Try to find dates without slashes (like "080826")
        if year is None:
            no_slash_match = re.search(MMDDYY_WITHOUT_SLASH_REGEX, bond_name)
            if no_slash_match:
                month, day, no_slash_year = no_slash_match.groups()
                # Make sure it looks like a reasonable date
                if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    year = no_slash_year
    
    return rate, year

def fallback_pattern_extraction(bond_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract rate and year using string manipulation as a fallback.
    
    Args:
        bond_name: The name of the bond
        
    Returns:
        Tuple of (interest_rate, year) where both could be None if not found
    """
    rate = None
    year = None
    
    # Look for percentage sign
    if '%' in bond_name:
        parts = bond_name.split('%')
        
        # Get the part before the % sign
        before_percent = parts[0].strip()
        words_before = before_percent.split()
        
        # The rate is likely the last word before the % sign
        if words_before:
            potential_rate = words_before[-1]
            if any(c.isdigit() for c in potential_rate):
                rate = potential_rate
        
        # Look for a year after the % sign
        after_percent = parts[1]
        for word in after_percent.split():
            word = word.strip('.,;:()')
            
            # Check for 4-digit year
            if word.isdigit() and len(word) == 4 and word.startswith('20'):
                year = word[2:]
                break
            
            # Check for 2-digit year
            if word.isdigit() and len(word) == 2:
                try:
                    if 0 <= int(word) <= 99:
                        year = word
                        break
                except ValueError:
                    pass
    
    return rate, year

def test_pattern_extraction():
    """Test our pattern extraction functions against the test bonds."""
    successes = 0
    total = len(TEST_BONDS)
    
    logger.info("Testing bond pattern extraction:")
    logger.info("-" * 50)
    
    for bond_name, expected_rate, expected_year in TEST_BONDS:
        # Try primary method
        rate, year = extract_pattern(bond_name)
        
        # If primary fails, try fallback
        if (rate is None and expected_rate is not None) or (year is None and expected_year is not None):
            fallback_rate, fallback_year = fallback_pattern_extraction(bond_name)
            rate = rate or fallback_rate
            year = year or fallback_year
        
        # Check if extraction matches expectations
        rate_match = (rate == expected_rate) or (rate is None and expected_rate is None)
        year_match = (year == expected_year) or (year is None and expected_year is None)
        
        if rate_match and year_match:
            result = "✓ PASS"
            successes += 1
        else:
            result = "✗ FAIL"
        
        logger.info(f"{result} | {bond_name}")
        logger.info(f"       Rate: expected={expected_rate}, got={rate}")
        logger.info(f"       Year: expected={expected_year}, got={year}")
        logger.info("-" * 50)
    
    # Print summary
    logger.info(f"Success rate: {successes}/{total} ({(successes/total*100):.1f}%)")
    
    # Generate a search pattern that would match this bond in a database
    logger.info("\nExample SQL pattern matches:")
    for bond_name, expected_rate, expected_year in TEST_BONDS:
        if expected_rate and expected_year:
            pattern = f"{expected_rate}%{' '}20{expected_year}"
            logger.info(f"{bond_name} -> SQL LIKE '%{pattern}%'")

if __name__ == "__main__":
    test_pattern_extraction()