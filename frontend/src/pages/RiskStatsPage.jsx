/**
 * Risk Statistics Page Component
 * 
 * Displays risk statistics and provides filtering options by asset class,
 * second level, and other criteria. Includes manual refresh button and
 * portfolio risk metrics analysis with performance optimizations.
 */
const React = window.React;
const { useState, useEffect } = React;
const RiskStatsPage = () => {
  // State for risk statistics data and filters
  const [riskStats, setRiskStats] = useState([]);
  const [status, setStatus] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  
  // Filters
  const [assetClass, setAssetClass] = useState('');
  const [secondLevel, setSecondLevel] = useState('');
  const [searchTerm, setSearchTerm] = useState('');
  
  // Portfolio risk metrics state
  const [portfolioRiskMetrics, setPortfolioRiskMetrics] = useState(null);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsError, setMetricsError] = useState(null);
  const [sampleSize, setSampleSize] = useState(2000); // Default sample size for large portfolios
  
  // Entity selection for risk metrics
  const [reportLevel, setReportLevel] = useState('client');
  const [levelKey, setLevelKey] = useState('All Clients');
  const [levelOptions, setLevelOptions] = useState([]);
  const [reportDate, setReportDate] = useState(new Date().toISOString().split('T')[0]); // Today's date
  
  // Available filter options
  const [secondLevelOptions, setSecondLevelOptions] = useState([]);
  
  // Load risk statistics on component mount and when filters change
  useEffect(() => {
    fetchRiskStats();
    fetchRiskStatsStatus();
    fetchEntityOptions('client');
  }, [assetClass, secondLevel]);
  
  // Fetch entity options for given level
  const fetchEntityOptions = async (level) => {
    try {
      const options = await window.api.getEntityOptions(level);
      setLevelOptions(options.map(opt => ({ value: opt.key, label: opt.display })));
    } catch (err) {
      console.error(`Error fetching ${level} options:`, err);
    }
  };
  
  // Fetch portfolio risk metrics
  const fetchPortfolioRiskMetrics = async () => {
    setMetricsLoading(true);
    setMetricsError(null);
    
    try {
      // For "All Clients" use sampling by default to prevent timeouts
      const actualSampleSize = (reportLevel === 'client' && levelKey === 'All Clients') 
        ? Math.max(2000, sampleSize) // Ensure a minimum sample size for All Clients
        : sampleSize;
      
      const metrics = await window.api.getPortfolioRiskMetrics(
        reportDate,
        reportLevel,
        levelKey,
        actualSampleSize
      );
      
      setPortfolioRiskMetrics(metrics);
    } catch (err) {
      console.error('Error fetching portfolio risk metrics:', err);
      setMetricsError(err.message || 'Failed to fetch portfolio risk metrics');
    } finally {
      setMetricsLoading(false);
    }
  };
  
  // Fetch risk statistics status
  const fetchRiskStatsStatus = async () => {
    try {
      const result = await window.api.getRiskStatsStatus();
      if (result.success) {
        setStatus(result);
      } else {
        setError(result.error || 'Failed to fetch risk statistics status');
      }
    } catch (err) {
      setError(err.message || 'An unexpected error occurred');
    }
  };
  
  // Fetch risk statistics based on current filters
  const fetchRiskStats = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await window.api.getRiskStats(
        assetClass || null,
        secondLevel || null,
        searchTerm || null
      );
      
      if (result.success) {
        setRiskStats(result.risk_stats || []);
        
        // Extract unique second level values for filter dropdown
        if (assetClass && result.risk_stats && result.risk_stats.length > 0) {
          const uniqueSecondLevels = [...new Set(
            result.risk_stats
              .filter(stat => stat.second_level)
              .map(stat => stat.second_level)
          )];
          setSecondLevelOptions(uniqueSecondLevels.sort());
        } else {
          setSecondLevelOptions([]);
        }
      } else {
        setError(result.error || 'Failed to fetch risk statistics');
        setRiskStats([]);
      }
    } catch (err) {
      setError(err.message || 'An unexpected error occurred');
      setRiskStats([]);
    } finally {
      setLoading(false);
    }
  };
  
  // Handle manual refresh of risk statistics
  const handleRefresh = async () => {
    setRefreshing(true);
    setError(null);
    
    try {
      const result = await window.api.updateRiskStats();
      
      if (result.success) {
        // Show success message
        // Then refresh the data
        fetchRiskStats();
        fetchRiskStatsStatus();
      } else {
        setError(result.error || 'Failed to update risk statistics');
      }
    } catch (err) {
      setError(err.message || 'An unexpected error occurred during refresh');
    } finally {
      setRefreshing(false);
    }
  };
  
  // Handle search input
  const handleSearch = (e) => {
    setSearchTerm(e.target.value);
  };
  
  // Handle search submission
  const handleSearchSubmit = (e) => {
    e.preventDefault();
    fetchRiskStats();
  };
  
  // Handle asset class change
  const handleAssetClassChange = (e) => {
    setAssetClass(e.target.value);
    setSecondLevel(''); // Reset second level when asset class changes
  };
  
  // Format date for display
  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };
  
  // Render table columns based on asset class
  const renderTableColumns = () => {
    if (assetClass === 'Equity') {
      return (
        <>
          <th className="px-4 py-2">Volatility</th>
          <th className="px-4 py-2">Beta</th>
        </>
      );
    } else if (assetClass === 'Fixed Income') {
      return (
        <>
          <th className="px-4 py-2">Duration</th>
        </>
      );
    } else {
      return (
        <>
          <th className="px-4 py-2">Risk Metric</th>
        </>
      );
    }
  };
  
  // Render table cell values based on asset class
  const renderTableCells = (stat) => {
    if (assetClass === 'Equity') {
      return (
        <>
          <td className="border px-4 py-2">{stat.volatility !== null ? stat.volatility.toFixed(2) : 'N/A'}</td>
          <td className="border px-4 py-2">{stat.beta !== null ? stat.beta.toFixed(2) : 'N/A'}</td>
        </>
      );
    } else if (assetClass === 'Fixed Income') {
      return (
        <>
          <td className="border px-4 py-2">{stat.duration !== null ? stat.duration.toFixed(2) : 'N/A'}</td>
        </>
      );
    } else {
      return (
        <>
          <td className="border px-4 py-2">
            {stat.beta !== null ? `Beta: ${stat.beta.toFixed(2)}` : 
             stat.volatility !== null ? `Vol: ${stat.volatility.toFixed(2)}` : 
             stat.duration !== null ? `Duration: ${stat.duration.toFixed(2)}` : 'N/A'}
          </td>
        </>
      );
    }
  };

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold text-green-900">Risk Statistics</h1>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="bg-green-700 hover:bg-green-800 text-white font-bold py-2 px-4 rounded flex items-center"
        >
          {refreshing ? (
            <>
              <svg className="animate-spin -ml-1 mr-2 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Updating...
            </>
          ) : (
            <>
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
              </svg>
              Refresh Risk Data
            </>
          )}
        </button>
      </div>
      
      {/* Status Information */}
      <div className="bg-gray-100 p-4 rounded-lg mb-6">
        <h2 className="text-lg font-semibold mb-2 text-green-800">Data Status</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="bg-white p-3 rounded shadow">
            <p className="text-sm text-gray-600">Last Updated</p>
            <p className="font-medium">{status.latest_import_date ? formatDate(status.latest_import_date) : 'Never'}</p>
          </div>
          <div className="bg-white p-3 rounded shadow">
            <p className="text-sm text-gray-600">Equity Records</p>
            <p className="font-medium">{status.equity_records || 0}</p>
          </div>
          <div className="bg-white p-3 rounded shadow">
            <p className="text-sm text-gray-600">Fixed Income Records</p>
            <p className="font-medium">{status.fixed_income_records || 0}</p>
          </div>
          <div className="bg-white p-3 rounded shadow">
            <p className="text-sm text-gray-600">Alternative Records</p>
            <p className="font-medium">{status.alternatives_records || 0}</p>
          </div>
        </div>
      </div>
      
      {/* Filters */}
      <div className="bg-white shadow rounded-lg p-4 mb-6">
        <h2 className="text-lg font-semibold mb-3 text-green-800">Filters</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Asset Class Filter */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="assetClass">
              Asset Class
            </label>
            <select
              id="assetClass"
              value={assetClass}
              onChange={handleAssetClassChange}
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            >
              <option value="">All Asset Classes</option>
              <option value="Equity">Equity</option>
              <option value="Fixed Income">Fixed Income</option>
              <option value="Alternatives">Alternatives</option>
            </select>
          </div>
          
          {/* Second Level Filter */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="secondLevel">
              Category
            </label>
            <select
              id="secondLevel"
              value={secondLevel}
              onChange={(e) => setSecondLevel(e.target.value)}
              disabled={!assetClass || secondLevelOptions.length === 0}
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            >
              <option value="">All Categories</option>
              {secondLevelOptions.map(level => (
                <option key={level} value={level}>{level}</option>
              ))}
            </select>
          </div>
          
          {/* Search */}
          <div>
            <form onSubmit={handleSearchSubmit}>
              <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="search">
                Search
              </label>
              <div className="flex">
                <input
                  id="search"
                  type="text"
                  value={searchTerm}
                  onChange={handleSearch}
                  placeholder="Search position or ticker..."
                  className="shadow appearance-none border rounded-l w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                />
                <button
                  type="submit"
                  className="bg-green-700 hover:bg-green-800 text-white font-bold py-2 px-4 rounded-r"
                >
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
                  </svg>
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
      
      {/* Error Display */}
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}
      
      {/* Portfolio Risk Metrics Section */}
      <div className="bg-white shadow rounded-lg p-4 mb-6">
        <h2 className="text-lg font-semibold mb-3 text-green-800">Portfolio Risk Metrics</h2>
        <p className="text-sm text-gray-600 mb-4">
          Analyze risk metrics for a portfolio or client, with performance optimizations for large portfolios.
        </p>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
          {/* Entity Level Selection */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="reportLevel">
              Entity Level
            </label>
            <select
              id="reportLevel"
              value={reportLevel}
              onChange={(e) => {
                const newLevel = e.target.value;
                setReportLevel(newLevel);
                fetchEntityOptions(newLevel);
              }}
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            >
              <option value="client">Client</option>
              <option value="portfolio">Portfolio</option>
              <option value="account">Account</option>
            </select>
          </div>
          
          {/* Entity Selection */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="levelKey">
              {reportLevel.charAt(0).toUpperCase() + reportLevel.slice(1)}
            </label>
            <select
              id="levelKey"
              value={levelKey}
              onChange={(e) => setLevelKey(e.target.value)}
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            >
              {levelOptions.map(option => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </div>
          
          {/* Report Date */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="reportDate">
              Report Date
            </label>
            <input
              id="reportDate"
              type="date"
              value={reportDate}
              onChange={(e) => setReportDate(e.target.value)}
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            />
          </div>
          
          {/* Sample Size */}
          <div>
            <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="sampleSize">
              Sample Size 
              <span className="text-xs text-gray-500 ml-1">(for large portfolios)</span>
            </label>
            <div className="flex">
              <input
                id="sampleSize"
                type="number"
                min="500"
                max="10000"
                step="500"
                value={sampleSize}
                onChange={(e) => setSampleSize(parseInt(e.target.value))}
                className="shadow appearance-none border rounded-l w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
              />
              <button
                onClick={fetchPortfolioRiskMetrics}
                disabled={metricsLoading}
                className="bg-green-700 hover:bg-green-800 text-white font-bold py-2 px-4 rounded-r"
              >
                {metricsLoading ? (
                  <svg className="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                ) : (
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13 9l3 3m0 0l-3 3m3-3H8m13 0a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                  </svg>
                )}
              </button>
            </div>
          </div>
        </div>
        
        {/* Metrics Error Display */}
        {metricsError && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
            <strong className="font-bold">Error: </strong>
            <span className="block sm:inline">{metricsError}</span>
          </div>
        )}
        
        {/* Portfolio Risk Metrics Display */}
        {portfolioRiskMetrics && (
          <div className="mt-4">
            <h3 className="text-md font-semibold mb-2 text-green-700">Results for {portfolioRiskMetrics.entity_name || levelKey}</h3>
            
            {/* Display metrics by asset class */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-2">
              {/* Equity Metrics */}
              <div className="bg-blue-50 p-3 rounded shadow">
                <h4 className="font-semibold text-blue-800 mb-2">Equity</h4>
                <div className="text-sm">
                  <p className="flex justify-between py-1 border-b border-blue-100">
                    <span>Weight:</span>
                    <span className="font-medium">{portfolioRiskMetrics.equity?.weight ? (portfolioRiskMetrics.equity.weight * 100).toFixed(2) + '%' : 'N/A'}</span>
                  </p>
                  <p className="flex justify-between py-1 border-b border-blue-100">
                    <span>Beta:</span>
                    <span className="font-medium">{portfolioRiskMetrics.equity?.beta ? portfolioRiskMetrics.equity.beta.toFixed(2) : 'N/A'}</span>
                  </p>
                  <p className="flex justify-between py-1 border-b border-blue-100">
                    <span>Volatility:</span>
                    <span className="font-medium">{portfolioRiskMetrics.equity?.volatility ? (portfolioRiskMetrics.equity.volatility * 100).toFixed(2) + '%' : 'N/A'}</span>
                  </p>
                </div>
              </div>
              
              {/* Fixed Income Metrics */}
              <div className="bg-red-50 p-3 rounded shadow">
                <h4 className="font-semibold text-red-800 mb-2">Fixed Income</h4>
                <div className="text-sm">
                  <p className="flex justify-between py-1 border-b border-red-100">
                    <span>Weight:</span>
                    <span className="font-medium">{portfolioRiskMetrics.fixed_income?.weight ? (portfolioRiskMetrics.fixed_income.weight * 100).toFixed(2) + '%' : 'N/A'}</span>
                  </p>
                  <p className="flex justify-between py-1 border-b border-red-100">
                    <span>Duration:</span>
                    <span className="font-medium">{portfolioRiskMetrics.fixed_income?.duration ? portfolioRiskMetrics.fixed_income.duration.toFixed(2) + ' years' : 'N/A'}</span>
                  </p>
                </div>
              </div>
              
              {/* Alternatives Metrics */}
              <div className="bg-orange-50 p-3 rounded shadow">
                <h4 className="font-semibold text-orange-800 mb-2">Alternatives</h4>
                <div className="text-sm">
                  <p className="flex justify-between py-1 border-b border-orange-100">
                    <span>Weight:</span>
                    <span className="font-medium">{portfolioRiskMetrics.alternatives?.weight ? (portfolioRiskMetrics.alternatives.weight * 100).toFixed(2) + '%' : 'N/A'}</span>
                  </p>
                </div>
              </div>
            </div>
            
            {/* Performance Info */}
            {portfolioRiskMetrics.performance_notes && (
              <div className="mt-4 bg-gray-50 p-3 rounded shadow">
                <h4 className="font-semibold text-gray-700 mb-1">Performance Notes</h4>
                <p className="text-sm text-gray-600">{portfolioRiskMetrics.performance_notes}</p>
                {portfolioRiskMetrics.sample_size && (
                  <p className="text-xs text-gray-500 mt-1">
                    Calculated using a sample of {portfolioRiskMetrics.sample_size} positions out of {portfolioRiskMetrics.total_positions} total.
                  </p>
                )}
              </div>
            )}
          </div>
        )}
      </div>
      
      {/* Risk Statistics Table */}
      <div className="bg-white shadow rounded-lg overflow-hidden">
        <div className="p-4 border-b">
          <h2 className="text-lg font-semibold text-green-800">Risk Statistics Data</h2>
          <p className="text-sm text-gray-600">
            {loading ? 'Loading statistics...' : 
             riskStats.length === 0 ? 'No risk statistics available with the current filters.' : 
             `Showing ${riskStats.length} risk statistics records`}
          </p>
        </div>
        
        {loading ? (
          <div className="flex justify-center items-center p-8">
            <svg className="animate-spin h-10 w-10 text-green-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
          </div>
        ) : riskStats.length === 0 ? (
          <div className="text-center p-8 text-gray-500">
            No risk statistics found. Try adjusting your filters or refreshing the risk data.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left">Position</th>
                  <th className="px-4 py-2">Ticker</th>
                  <th className="px-4 py-2">Asset Class</th>
                  <th className="px-4 py-2">Category</th>
                  {renderTableColumns()}
                  <th className="px-4 py-2">Import Date</th>
                </tr>
              </thead>
              <tbody>
                {riskStats.map((stat, index) => (
                  <tr key={index} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                    <td className="border px-4 py-2 font-medium">{stat.position}</td>
                    <td className="border px-4 py-2 text-center">{stat.ticker_symbol || 'N/A'}</td>
                    <td className="border px-4 py-2 text-center">{stat.asset_class}</td>
                    <td className="border px-4 py-2">{stat.second_level || 'N/A'}</td>
                    {renderTableCells(stat)}
                    <td className="border px-4 py-2 text-sm text-gray-600">{formatDate(stat.import_date)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

