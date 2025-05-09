// RiskStatsJobManager.jsx
// Non-module version for direct browser loading

// Use React and Chakra UI from global scope
const { useState, useEffect } = React;
const { 
  Alert, 
  Button, 
  Card, 
  Progress, 
  Stat, 
  StatLabel, 
  StatNumber, 
  Stack,
  HStack,
  VStack,
  Text,
  Badge,
  Box,
  Heading,
  Divider,
  useToast
} = ChakraUI;

// API functions are available from the window.api global object

/**
 * Component for managing risk stats jobs with improved UX for job tracking.
 * This component allows users to trigger risk stats updates and monitor processing status in real-time.
 */
const RiskStatsJobManager = () => {
  const [jobState, setJobState] = useState({
    isLoading: false,
    currentJob: null,
    error: null,
    statsStatus: null
  });
  
  const toast = useToast();
  
  // Poll for updates if a job is running
  useEffect(() => {
    let interval;
    
    if (jobState.currentJob && ['pending', 'running'].includes(jobState.currentJob.status)) {
      interval = setInterval(() => {
        checkJobStatus(jobState.currentJob.job_id);
      }, 2000); // Poll every 2 seconds
    }
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [jobState.currentJob]);
  
  // Load initial stats status
  useEffect(() => {
    fetchRiskStatsStatus();
  }, []);
  
  // Start a new risk stats update job
  const startRiskStatsJob = async () => {
    try {
      setJobState(prev => ({ ...prev, isLoading: true, error: null }));
      
      const result = await window.api.updateRiskStats();
      
      if (result.success) {
        setJobState(prev => ({ 
          ...prev, 
          isLoading: false, 
          currentJob: result
        }));
        
        toast({
          title: 'Job Started',
          description: `Risk stats update job #${result.job_id} started successfully`,
          status: 'success',
          duration: 5000,
          isClosable: true,
        });
      } else {
        throw new Error(result.error || 'Failed to start job');
      }
    } catch (error) {
      setJobState(prev => ({ 
        ...prev, 
        isLoading: false, 
        error: error.message || 'Failed to start risk stats update' 
      }));
      
      toast({
        title: 'Error',
        description: error.message || 'Failed to start risk stats update',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    }
  };
  
  // Check the status of a running job
  const checkJobStatus = async (jobId) => {
    try {
      const result = await window.api.getRiskStatsJobStatus(jobId);
      
      if (result.success) {
        setJobState(prev => ({ ...prev, currentJob: result }));
        
        // Job completed, refresh stats status
        if (result.status === 'completed') {
          fetchRiskStatsStatus();
          
          toast({
            title: 'Job Completed',
            description: `Processed ${result.total_records} records in ${result.duration_seconds?.toFixed(2) || '?'} seconds`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        } else if (result.status === 'failed') {
          toast({
            title: 'Job Failed',
            description: result.error_message || 'Risk stats update failed',
            status: 'error',
            duration: 5000,
            isClosable: true,
          });
        }
      }
    } catch (error) {
      console.error('Error checking job status:', error);
    }
  };
  
  // Fetch the current risk stats status
  const fetchRiskStatsStatus = async () => {
    try {
      const status = await window.api.getRiskStatsStatus();
      setJobState(prev => ({ ...prev, statsStatus: status }));
    } catch (error) {
      console.error('Error fetching risk stats status:', error);
    }
  };
  
  // Helper to format the status badge color
  const getStatusColor = (status) => {
    switch (status) {
      case 'pending': return 'yellow';
      case 'running': return 'blue';
      case 'completed': return 'green';
      case 'failed': return 'red';
      default: return 'gray';
    }
  };
  
  // Calculate progress percentage
  const calculateProgress = (job) => {
    if (!job) return 0;
    
    // Return 100% if completed
    if (job.status === 'completed') return 100;
    
    // Different progress metrics based on job status
    if (job.status === 'running') {
      // If the job reports actual progress, use that
      if (job.progress_percent) return job.progress_percent;
      
      // Use count-based estimate with some ranges
      const totalRecords = job.total_records || 0;
      
      if (totalRecords > 0) {
        // For jobs that are actually adding records, scale progress reasonably
        if (totalRecords <= 100) return Math.min(50 + (totalRecords / 2), 90);
        if (totalRecords <= 1000) return Math.min(70 + (totalRecords / 100), 95);
        return Math.min(85 + (totalRecords / 1000), 98); // Approaching completion
      }
      
      // Default indeterminate progress
      return 25; // Show some progress but not too much
    }
    
    // Pending status
    if (job.status === 'pending') return 10;
    
    // Default
    return 0;
  };
  
  const { isLoading, currentJob, error, statsStatus } = jobState;
  
  return (
    <Card p={4}>
      <VStack spacing={4} align="stretch">
        <Heading size="md">Risk Statistics Management</Heading>
        <Divider />
        
        {/* Current stats info */}
        {statsStatus && statsStatus.has_data && (
          <Box p={3} bg="gray.50" borderRadius="md">
            <HStack justify="space-between" wrap="wrap">
              <Stat>
                <StatLabel>Last Updated</StatLabel>
                <StatNumber fontSize="md">{new Date(statsStatus.latest_import_date).toLocaleDateString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Total Records</StatLabel>
                <StatNumber fontSize="md">{statsStatus.total_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Equity</StatLabel>
                <StatNumber fontSize="md">{statsStatus.equity_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Fixed Income</StatLabel>
                <StatNumber fontSize="md">{statsStatus.fixed_income_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Alternatives</StatLabel>
                <StatNumber fontSize="md">{statsStatus.alternatives_records.toLocaleString()}</StatNumber>
              </Stat>
            </HStack>
            
            {statsStatus.job_completed_at && (
              <Text mt={2} fontSize="sm" color="gray.600">
                Last job completed at {new Date(statsStatus.job_completed_at).toLocaleString()}
                {statsStatus.job_duration_seconds && ` (${statsStatus.job_duration_seconds.toFixed(2)}s)`}
              </Text>
            )}
          </Box>
        )}
        
        {/* Error alert */}
        {error && (
          <Alert status="error">{error}</Alert>
        )}
        
        {/* Current job status */}
        {currentJob && (
          <Box p={4} borderWidth="1px" borderRadius="md">
            <HStack justify="space-between" mb={2}>
              <Text fontWeight="bold">Job #{currentJob.job_id}</Text>
              <Badge colorScheme={getStatusColor(currentJob.status)}>
                {currentJob.status.toUpperCase()}
              </Badge>
            </HStack>
            
            <Progress 
              value={calculateProgress(currentJob)} 
              size="sm" 
              colorScheme={getStatusColor(currentJob.status)}
              isIndeterminate={['pending', 'running'].includes(currentJob.status)}
              mb={2}
            />
            
            <Stack direction={{ base: 'column', md: 'row' }} spacing={4} mt={3}>
              {currentJob.total_records > 0 && (
                <Stat size="sm">
                  <StatLabel>Records</StatLabel>
                  <StatNumber fontSize="sm">{currentJob.total_records}</StatNumber>
                </Stat>
              )}
              
              {currentJob.duration_seconds && (
                <Stat size="sm">
                  <StatLabel>Duration</StatLabel>
                  <StatNumber fontSize="sm">{currentJob.duration_seconds.toFixed(2)}s</StatNumber>
                </Stat>
              )}
              
              {currentJob.memory_usage_mb && (
                <Stat size="sm">
                  <StatLabel>Memory</StatLabel>
                  <StatNumber fontSize="sm">{currentJob.memory_usage_mb.toFixed(1)} MB</StatNumber>
                </Stat>
              )}
            </Stack>
            
            {currentJob.error_message && (
              <Alert status="error" mt={2} size="sm">
                {currentJob.error_message}
              </Alert>
            )}
          </Box>
        )}
        
        {/* Action buttons */}
        <HStack justify="flex-end" spacing={4} mt={2}>
          <Button 
            onClick={fetchRiskStatsStatus} 
            colorScheme="blue" 
            variant="outline" 
            size="sm"
          >
            Refresh Status
          </Button>
          
          <Button 
            onClick={startRiskStatsJob} 
            colorScheme="green" 
            isLoading={isLoading}
            isDisabled={currentJob && ['pending', 'running'].includes(currentJob.status)}
            size="sm"
          >
            Update Risk Stats
          </Button>
        </HStack>
      </VStack>
    </Card>
  );
};

// Make component available globally
window.RiskStatsJobManager = RiskStatsJobManager;