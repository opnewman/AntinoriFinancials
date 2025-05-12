import requests
import json

def test_risk_metrics_api():
    # URL with parameters
    base_url = "http://localhost:5000/api/portfolio/risk-metrics"
    # Try with another client
    params = {
        "level": "client",
        "level_key": "The Linden East II Trust (Abigail Wexner)",
        "date": "2025-05-01"
    }
    
    # Make the request
    print(f"Making request to {base_url} with params {params}")
    
    try:
        response = requests.get(base_url, params=params)
        
        # Print response information
        print(f"Status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
        
        # Try to parse JSON only if we get a successful response
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"Response body (JSON):\n{json.dumps(data, indent=2)}")
            except json.JSONDecodeError:
                print(f"Response body (not JSON):\n{response.text[:500]}")
        else:
            print(f"Response body (error):\n{response.text[:500]}")
            
    except requests.RequestException as e:
        print(f"Request error: {e}")

if __name__ == "__main__":
    test_risk_metrics_api()