import os
import pytest
import json
from fastapi.testclient import TestClient
from datetime import date, timedelta
import io

# Import the FastAPI app
from main import app

# Create test client
client = TestClient(app)

# Test data paths
TEST_DATA_DIR = "data/sample_data"
TEST_DATA_DUMP = os.path.join(TEST_DATA_DIR, "data_dump.xlsx")
TEST_OWNERSHIP = os.path.join(TEST_DATA_DIR, "ownership.xlsx")
TEST_RISK_STATS = os.path.join(TEST_DATA_DIR, "risk_stats.xlsx")

# Check if test data exists
test_data_exists = (
    os.path.exists(TEST_DATA_DUMP) 
    and os.path.exists(TEST_OWNERSHIP) 
    and os.path.exists(TEST_RISK_STATS)
)

# Basic health check test
def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

# Root endpoint test
def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()

@pytest.mark.skipif(not test_data_exists, reason="Test data files not found")
def test_upload_data_dump():
    with open(TEST_DATA_DUMP, "rb") as f:
        response = client.post(
            "/api/upload/data_dump",
            files={"file": ("data_dump.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert "rows_processed" in result
    assert "rows_inserted" in result

@pytest.mark.skipif(not test_data_exists, reason="Test data files not found")
def test_upload_ownership_tree():
    with open(TEST_OWNERSHIP, "rb") as f:
        response = client.post(
            "/api/upload/ownership_tree",
            files={"file": ("ownership.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert "rows_processed" in result
    assert "rows_inserted" in result

@pytest.mark.skipif(not test_data_exists, reason="Test data files not found")
def test_upload_security_risk_stats():
    with open(TEST_RISK_STATS, "rb") as f:
        response = client.post(
            "/api/upload/security_risk_stats",
            files={"file": ("risk_stats.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert "rows_processed" in result
    assert "rows_inserted" in result

def test_get_ownership_tree():
    # First, check if data exists in the system
    response = client.get("/api/ownership_tree")
    
    # If data exists, validate structure
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)
        
        if len(data) > 0:
            # Check structure of first client
            client_data = data[0]
            assert "id" in client_data
            assert "name" in client_data
            assert "type" in client_data
            assert "children" in client_data
            assert client_data["type"] == "client"
    
    # If no data, should return 404
    elif response.status_code == 404:
        assert response.json()["detail"] == "No ownership data found"

def test_generate_portfolio_report():
    # Try to get a report - this might fail if no data has been uploaded yet
    today = date.today().isoformat()
    
    # Try with a client level report
    response = client.get(
        "/api/generate_portfolio_report", 
        params={"date": today, "level": "client", "level_key": "Sprackman Family"}
    )
    
    # If we have data, validate the response structure
    if response.status_code == 200:
        data = response.json()
        assert "report_date" in data
        assert "level" in data
        assert "level_key" in data
        assert "total_adjusted_value" in data
        assert "asset_allocation" in data
        assert "liquidity" in data
        assert "performance" in data
        assert "risk_metrics" in data
        
        # Check performance structure
        assert isinstance(data["performance"], list)
        if len(data["performance"]) > 0:
            assert "period" in data["performance"][0]
            assert "value" in data["performance"][0]
            assert "percentage" in data["performance"][0]
        
        # Check risk metrics structure
        assert isinstance(data["risk_metrics"], list)
        if len(data["risk_metrics"]) > 0:
            assert "metric" in data["risk_metrics"][0]
            assert "value" in data["risk_metrics"][0]

def test_chart_endpoints():
    # Try to get chart data for a client
    today = date.today().isoformat()
    
    # Try allocations chart
    response = client.get(
        "/api/portfolio_report/chart/allocations", 
        params={"date": today, "level": "client", "level_key": "Sprackman Family"}
    )
    
    # If we have data, validate chart structure
    if response.status_code == 200:
        data = response.json()
        assert "labels" in data
        assert "datasets" in data
        assert isinstance(data["labels"], list)
        assert isinstance(data["datasets"], list)
        
        if len(data["datasets"]) > 0:
            assert "data" in data["datasets"][0]
            assert "backgroundColor" in data["datasets"][0]
    
    # Try liquidity chart
    response = client.get(
        "/api/portfolio_report/chart/liquidity", 
        params={"date": today, "level": "client", "level_key": "Sprackman Family"}
    )
    
    # If we have data, validate chart structure
    if response.status_code == 200:
        data = response.json()
        assert "labels" in data
        assert "datasets" in data
        
    # Try performance chart
    response = client.get(
        "/api/portfolio_report/chart/performance", 
        params={"date": today, "level": "client", "level_key": "Sprackman Family", "period": "YTD"}
    )
    
    # If we have data, validate chart structure
    if response.status_code == 200:
        data = response.json()
        assert "labels" in data
        assert "datasets" in data
        assert isinstance(data["datasets"], list)
        
        if len(data["datasets"]) > 0:
            assert "label" in data["datasets"][0]
            assert "data" in data["datasets"][0]
            assert "borderColor" in data["datasets"][0]
