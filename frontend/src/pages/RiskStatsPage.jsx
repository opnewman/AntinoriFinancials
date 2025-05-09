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
    <Container maxW="container.xl" py={5}>
      <Heading mb={4}>Risk Statistics</Heading>
      <Text mb={6}>
        Manage and view security risk metrics including volatility, beta, and duration data.
      </Text>
      
      <SimpleGrid columns={{ base: 1, lg: 2 }} spacing={6} mb={6}>
        <Box>
          <RiskStatsJobManager />
        </Box>
        
        <Card>
          <CardHeader>
            <Heading size="md">Filter Risk Statistics</Heading>
          </CardHeader>
          <CardBody>
            <Stack spacing={4}>
              <SimpleGrid columns={{ base: 1, md: 2 }} spacing={4}>
                <FormControl>
                  <FormLabel>Asset Class</FormLabel>
                  <Select 
                    name="assetClass"
                    value={filters.assetClass}
                    onChange={handleFilterChange}
                    placeholder="All Asset Classes"
                  >
                    <option value="Equity">Equity</option>
                    <option value="Fixed Income">Fixed Income</option>
                    <option value="Alternatives">Alternatives</option>
                  </Select>
                </FormControl>
                
                <FormControl>
                  <FormLabel>Second Level</FormLabel>
                  <Input
                    name="secondLevel"
                    value={filters.secondLevel}
                    onChange={handleFilterChange}
                    placeholder="Filter by second level classification"
                  />
                </FormControl>
              </SimpleGrid>
              
              <SimpleGrid columns={{ base: 1, md: 2 }} spacing={4}>
                <FormControl>
                  <FormLabel>Position/Security</FormLabel>
                  <Input
                    name="position"
                    value={filters.position}
                    onChange={handleFilterChange}
                    placeholder="Filter by security name"
                  />
                </FormControl>
                
                <FormControl>
                  <FormLabel>Ticker Symbol</FormLabel>
                  <Input
                    name="ticker"
                    value={filters.ticker}
                    onChange={handleFilterChange}
                    placeholder="Filter by ticker symbol"
                  />
                </FormControl>
              </SimpleGrid>
              
              <Button 
                colorScheme="blue" 
                onClick={() => fetchRiskStats()} 
                isLoading={loading}
              >
                Apply Filters
              </Button>
            </Stack>
          </CardBody>
        </Card>
      </SimpleGrid>
      
      {error && (
        <Alert status="error" mb={4}>
          <AlertIcon />
          {error}
        </Alert>
      )}
      
      <Card variant="outline">
        <Box overflowX="auto">
          <Table variant="simple">
            <Thead>
              <Tr>
                <Th>Position</Th>
                <Th>Ticker</Th>
                <Th>Asset Class</Th>
                <Th>Second Level</Th>
                <Th>Beta</Th>
                <Th>Volatility</Th>
                <Th>Duration</Th>
              </Tr>
            </Thead>
            <Tbody>
              {loading ? (
                <Tr>
                  <Td colSpan={7} textAlign="center" py={10}>
                    <Spinner />
                  </Td>
                </Tr>
              ) : riskStats.length === 0 ? (
                <Tr>
                  <Td colSpan={7} textAlign="center" py={10}>
                    No risk statistics found matching your filters
                  </Td>
                </Tr>
              ) : (
                riskStats.map(stat => (
                  <Tr key={stat.id}>
                    <Td>{stat.position}</Td>
                    <Td>{stat.ticker_symbol || '-'}</Td>
                    <Td>
                      <Badge 
                        colorScheme={
                          stat.asset_class === 'Equity' ? 'blue' : 
                          stat.asset_class === 'Fixed Income' ? 'red' : 
                          stat.asset_class === 'Alternatives' ? 'orange' : 
                          'gray'
                        }
                      >
                        {stat.asset_class}
                      </Badge>
                    </Td>
                    <Td>{stat.second_level || '-'}</Td>
                    <Td>{formatValue(stat.beta, 'decimal')}</Td>
                    <Td>{formatValue(stat.volatility, 'decimal')}</Td>
                    <Td>{formatValue(stat.duration, 'decimal')}</Td>
                  </Tr>
                ))
              )}
            </Tbody>
          </Table>
        </Box>
      </Card>
      
      {/* Pagination controls */}
      <Flex justify="space-between" mt={4}>
        <Text>
          Showing {pagination.offset + 1} - {Math.min(pagination.offset + riskStats.length, pagination.count)} of {pagination.count} records
        </Text>
        <HStack>
          <Button
            onClick={handlePrevPage}
            isDisabled={pagination.offset === 0 || loading}
            size="sm"
          >
            Previous
          </Button>
          <Button
            onClick={handleNextPage}
            isDisabled={!pagination.hasMore || loading}
            size="sm"
          >
            Next
          </Button>
        </HStack>
      </Flex>
    </Container>
  );
};

export default RiskStatsPage;