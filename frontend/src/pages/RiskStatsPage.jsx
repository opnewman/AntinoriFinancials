// RiskStatsPage.jsx
// Non-module version for direct browser loading

// Get React and Chakra UI from global scope
const { useState, useEffect } = React;
const { 
  Box, 
  Container, 
  Heading, 
  Text, 
  Tabs, 
  TabList, 
  TabPanels, 
  Tab, 
  TabPanel,
  SimpleGrid,
  Spinner,
  Alert,
  AlertIcon,
  Input,
  Select,
  Button,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  FormControl,
  FormLabel,
  Stack,
  Flex,
  HStack,
  Badge,
  useToast,
  Card,
  CardHeader,
  CardBody
} = ChakraUI;

// RiskStatsJobManager component is loaded globally from a script tag

/**
 * Risk Statistics page for viewing and managing security risk metrics
 */
const RiskStatsPage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [riskStats, setRiskStats] = useState([]);
  const [unmatchedSecurities, setUnmatchedSecurities] = useState({});
  const [loadingUnmatched, setLoadingUnmatched] = useState(false);
  const [activeTab, setActiveTab] = useState(0); // 0 = Risk Stats, 1 = Unmatched Securities
  const [filters, setFilters] = useState({
    assetClass: '',
    secondLevel: '',
    position: '',
    ticker: ''
  });
  const [pagination, setPagination] = useState({
    limit: 25,
    offset: 0,
    count: 0,
    hasMore: false
  });
  
  const toast = useToast();
  
  // Load initial data
  useEffect(() => {
    fetchRiskStats();
  }, [filters, pagination.offset, pagination.limit]);
  
  // Load unmatched securities when the tab is selected
  useEffect(() => {
    if (activeTab === 1) {
      fetchUnmatchedSecurities();
    }
  }, [activeTab]);
  
  const fetchRiskStats = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const { assetClass, secondLevel, position, ticker } = filters;
      const response = await window.api.getRiskStats(
        assetClass || null,
        secondLevel || null, 
        position || null, 
        ticker || null
      );
      
      if (response.success) {
        setRiskStats(response.records || []);
        setPagination({
          ...pagination,
          count: response.count || 0,
          hasMore: response.has_more || false
        });
      } else {
        setError(response.error || 'Failed to load risk statistics');
        toast({
          title: 'Error',
          description: response.error || 'Failed to load risk statistics',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      }
    } catch (err) {
      setError(err.message || 'An unexpected error occurred');
      toast({
        title: 'Error',
        description: err.message || 'An unexpected error occurred',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setLoading(false);
    }
  };
  
  // Handle filter changes
  const handleFilterChange = (e) => {
    const { name, value } = e.target;
    setFilters(prev => ({
      ...prev,
      [name]: value
    }));
    
    // Reset pagination when filters change
    setPagination(prev => ({
      ...prev,
      offset: 0
    }));
  };
  
  // Handle pagination
  const handleNextPage = () => {
    if (pagination.hasMore) {
      setPagination(prev => ({
        ...prev,
        offset: prev.offset + prev.limit
      }));
    }
  };
  
  const handlePrevPage = () => {
    if (pagination.offset > 0) {
      setPagination(prev => ({
        ...prev,
        offset: Math.max(0, prev.offset - prev.limit)
      }));
    }
  };
  
  // Fetch unmatched securities from the API
  const fetchUnmatchedSecurities = async () => {
    setLoadingUnmatched(true);
    
    try {
      const response = await window.api.getUnmatchedSecurities();
      
      if (response.success) {
        setUnmatchedSecurities(response.unmatched_securities || {});
      } else {
        toast({
          title: 'Error',
          description: response.error || 'Failed to load unmatched securities',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      }
    } catch (err) {
      console.error('Error fetching unmatched securities:', err);
      toast({
        title: 'Error',
        description: err.message || 'An unexpected error occurred',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setLoadingUnmatched(false);
    }
  };
  
  // Format values for display
  const formatValue = (value, type = 'number') => {
    if (value === null || value === undefined) return '-';
    
    if (type === 'percent') {
      return `${parseFloat(value).toFixed(2)}%`;
    }
    
    if (type === 'decimal') {
      return parseFloat(value).toFixed(2);
    }
    
    return value;
  };
  
  return (
    <div className="container mx-auto px-4 py-5">
      <h2 className="text-2xl font-bold mb-4">Risk Statistics</h2>
      <p className="text-gray-600 mb-6">
        Manage and view security risk metrics including volatility, beta, and duration data.
      </p>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <div>
          <RiskStatsJobManager />
        </div>
        
        <div className="bg-white rounded-lg shadow-md">
          <div className="border-b pb-2 mb-3 p-4">
            <h2 className="font-bold text-xl">Filter Risk Statistics</h2>
          </div>
          <div className="p-4">
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Asset Class</label>
                  <select 
                    name="assetClass"
                    value={filters.assetClass}
                    onChange={handleFilterChange}
                    className="border border-gray-300 rounded p-2 w-full"
                  >
                    <option value="">All Asset Classes</option>
                    <option value="Equity">Equity</option>
                    <option value="Fixed Income">Fixed Income</option>
                    <option value="Alternatives">Alternatives</option>
                  </select>
                </div>
                
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Second Level</label>
                  <input
                    type="text"
                    name="secondLevel"
                    value={filters.secondLevel}
                    onChange={handleFilterChange}
                    placeholder="Filter by second level classification"
                    className="border border-gray-300 rounded p-2 w-full"
                  />
                </div>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Position/Security</label>
                  <input
                    type="text"
                    name="position"
                    value={filters.position}
                    onChange={handleFilterChange}
                    placeholder="Filter by security name"
                    className="border border-gray-300 rounded p-2 w-full"
                  />
                </div>
                
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Ticker Symbol</label>
                  <input
                    type="text"
                    name="ticker"
                    value={filters.ticker}
                    onChange={handleFilterChange}
                    placeholder="Filter by ticker symbol"
                    className="border border-gray-300 rounded p-2 w-full"
                  />
                </div>
              </div>
              
              <button 
                className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded font-semibold"
                onClick={() => fetchRiskStats()}
                disabled={loading}
              >
                {loading ? (
                  <span>
                    <span className="inline-block w-4 h-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin"></span>
                    Loading...
                  </span>
                ) : 'Apply Filters'}
              </button>
            </div>
          </div>
        </div>
      </div>
      
      {error && (
        <div className="rounded-md p-4 border bg-red-100 text-red-800 border-red-200 mb-4">
          ⚠️ {error}
        </div>
      )}
      
      {/* Tab Navigation */}
      <div className="mb-4">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex">
            <button
              className={`mr-8 py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 0
                  ? 'border-green-500 text-green-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
              onClick={() => setActiveTab(0)}
            >
              Risk Statistics
            </button>
            <button
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 1
                  ? 'border-green-500 text-green-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
              onClick={() => setActiveTab(1)}
            >
              Unmatched Securities
            </button>
          </nav>
        </div>
      </div>
      
      {/* Risk Stats Tab Content */}
      {activeTab === 0 && (
        <>
          <div className="bg-white rounded-lg shadow-md border">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Position</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Ticker</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Asset Class</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Second Level</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Beta</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Volatility</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Duration</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {loading ? (
                    <tr>
                      <td colSpan={7} className="px-6 py-4 whitespace-nowrap text-center py-10">
                        <div className="inline-block w-6 h-6 border-2 border-gray-300 border-t-blue-600 rounded-full animate-spin"></div>
                      </td>
                    </tr>
                  ) : riskStats.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="px-6 py-4 whitespace-nowrap text-center py-10">
                        No risk statistics found matching your filters
                      </td>
                    </tr>
                  ) : (
                    riskStats.map(stat => (
                      <tr key={stat.id}>
                        <td className="px-6 py-4 whitespace-nowrap">{stat.position}</td>
                        <td className="px-6 py-4 whitespace-nowrap">{stat.ticker_symbol || '-'}</td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`inline-block px-2 py-1 text-xs font-semibold rounded-full ${
                            stat.asset_class === 'Equity' ? 'bg-blue-100 text-blue-800' : 
                            stat.asset_class === 'Fixed Income' ? 'bg-red-100 text-red-800' : 
                            stat.asset_class === 'Alternatives' ? 'bg-orange-100 text-orange-800' : 
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {stat.asset_class}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">{stat.second_level || '-'}</td>
                        <td className="px-6 py-4 whitespace-nowrap">{formatValue(stat.beta, 'decimal')}</td>
                        <td className="px-6 py-4 whitespace-nowrap">{formatValue(stat.volatility, 'decimal')}</td>
                        <td className="px-6 py-4 whitespace-nowrap">{formatValue(stat.duration, 'decimal')}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
          
          {/* Pagination controls */}
          <div className="flex justify-between mt-4">
            <p className="text-gray-600">
              Showing {pagination.offset + 1} - {Math.min(pagination.offset + riskStats.length, pagination.count)} of {pagination.count} records
            </p>
            <div className="flex space-x-4">
              <button
                onClick={handlePrevPage}
                disabled={pagination.offset === 0 || loading}
                className="px-2 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Previous
              </button>
              <button
                onClick={handleNextPage}
                disabled={!pagination.hasMore || loading}
                className="px-2 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
          </div>
        </>
      )}
      
      {/* Unmatched Securities Tab Content */}
      {activeTab === 1 && (
        <div className="bg-white rounded-lg shadow-md border">
          {loadingUnmatched ? (
            <div className="p-12 text-center">
              <div className="inline-block w-8 h-8 border-2 border-gray-300 border-t-green-600 rounded-full animate-spin mb-4"></div>
              <p>Loading unmatched securities...</p>
            </div>
          ) : (
            <>
              <div className="p-4 border-b">
                <h3 className="text-lg font-semibold">Unmatched Securities</h3>
                <p className="text-sm text-gray-600">Securities in your portfolio that don't have matching risk statistics.</p>
              </div>
              
              <div className="p-4">
                {Object.keys(unmatchedSecurities).length === 0 ? (
                  <div className="text-center p-6">
                    <p className="text-gray-500">No unmatched securities found</p>
                  </div>
                ) : (
                  <div>
                    {Object.entries(unmatchedSecurities).map(([assetClass, securities]) => (
                      <div key={assetClass} className="mb-6">
                        <h4 className={`mb-2 text-md font-semibold ${
                          assetClass === 'equity' ? 'text-blue-700' : 
                          assetClass === 'fixed_income' ? 'text-red-700' : 
                          assetClass === 'alternatives' ? 'text-orange-700' :
                          assetClass === 'hard_currency' ? 'text-yellow-700' : 
                          'text-gray-700'
                        }`}>
                          {assetClass.charAt(0).toUpperCase() + assetClass.slice(1).replace('_', ' ')} 
                          <span className="ml-2 text-sm font-normal text-gray-500">({securities.length} securities)</span>
                        </h4>
                        
                        <div className="overflow-x-auto">
                          <table className="min-w-full divide-y divide-gray-200 text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Security Name</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-200">
                              {securities.map((security, index) => (
                                <tr key={index} className="hover:bg-gray-50">
                                  <td className="px-4 py-2 whitespace-nowrap">{security}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
};

// Make component available globally
window.RiskStatsPage = RiskStatsPage;