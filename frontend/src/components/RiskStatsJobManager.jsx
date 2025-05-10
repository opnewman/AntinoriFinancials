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
  
  // Create a simple toast function if ChakraUI's useToast is not available
  const toast = typeof useToast === 'function' ? useToast() : {
    toast: (props) => {
      console.log(`Toast: ${props.title} - ${props.description}`);
      // Fallback to browser alert for crucial messages
      if (props.status === 'error') {
        alert(`Error: ${props.description}`);
      }
    }
  };
  
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
        
        if (typeof toast === 'function') {
          toast({
            title: 'Job Started',
            description: `Risk stats update job #${result.job_id} started successfully`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        } else if (toast.toast) {
          toast.toast({
            title: 'Job Started',
            description: `Risk stats update job #${result.job_id} started successfully`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        }
      } else {
        throw new Error(result.error || 'Failed to start job');
      }
    } catch (error) {
      setJobState(prev => ({ 
        ...prev, 
        isLoading: false, 
        error: error.message || 'Failed to start risk stats update' 
      }));
      
      if (typeof toast === 'function') {
        toast({
          title: 'Error',
          description: error.message || 'Failed to start risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      } else if (toast.toast) {
        toast.toast({
          title: 'Error',
          description: error.message || 'Failed to start risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      }
    }
  };
  
  // Start a high-performance optimized risk stats update (~20 seconds)
  const startOptimizedRiskStatsUpdate = async () => {
    try {
      setJobState(prev => ({ ...prev, isLoading: true, error: null }));
      
      const result = await window.api.updateRiskStatsOptimized();
      
      if (result.success) {
        // For optimized updates, we don't have a job ID since they complete immediately
        // Create a synthetic job object with completed status
        const completedJob = {
          success: true,
          status: 'completed',
          total_records: result.total_records || result.processed_records || 0,
          duration_seconds: result.processing_time_seconds || result.total_api_time_seconds || 0,
          stats: {
            equity_records: result.equity_records || 0,
            fixed_income_records: result.fixed_income_records || 0,
            alternatives_records: result.alternatives_records || 0
          }
        };
        
        setJobState(prev => ({ 
          ...prev, 
          isLoading: false, 
          currentJob: completedJob
        }));
        
        // Immediately refresh status after completion
        fetchRiskStatsStatus();
        
        if (typeof toast === 'function') {
          toast({
            title: 'High-Performance Update Complete',
            description: `Processed ${result.total_records || result.processed_records || 0} records in ${(result.processing_time_seconds || result.total_api_time_seconds || 0).toFixed(2)} seconds`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        } else if (toast.toast) {
          toast.toast({
            title: 'High-Performance Update Complete',
            description: `Processed ${result.total_records || result.processed_records || 0} records in ${(result.processing_time_seconds || result.total_api_time_seconds || 0).toFixed(2)} seconds`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        }
      } else {
        throw new Error(result.error || 'Failed to start high-performance update');
      }
    } catch (error) {
      setJobState(prev => ({ 
        ...prev, 
        isLoading: false, 
        error: error.message || 'Failed to start optimized risk stats update' 
      }));
      
      if (typeof toast === 'function') {
        toast({
          title: 'Error',
          description: error.message || 'Failed to start optimized risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      } else if (toast.toast) {
        toast.toast({
          title: 'Error',
          description: error.message || 'Failed to start optimized risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      }
    }
  };
  

        
        setJobState(prev => ({ 
          ...prev, 
          isLoading: false, 
          currentJob: completedJob
        }));
        
        // Immediately refresh status after completion
        fetchRiskStatsStatus();
        
        if (typeof toast === 'function') {
          toast({
            title: 'High-Performance Update Complete',
            description: `Processed ${result.total_records || result.processed_records || 0} records in ${(result.processing_time_seconds || result.total_api_time_seconds || 0).toFixed(2)} seconds`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        } else if (toast.toast) {
          toast.toast({
            title: 'High-Performance Update Complete',
            description: `Processed ${result.total_records || result.processed_records || 0} records in ${(result.processing_time_seconds || result.total_api_time_seconds || 0).toFixed(2)} seconds`,
            status: 'success',
            duration: 5000,
            isClosable: true,
          });
        }
      } else {
        throw new Error(result.error || 'Failed to start optimized update');
      }
    } catch (error) {
      setJobState(prev => ({ 
        ...prev, 
        isLoading: false, 
        error: error.message || 'Failed to start optimized risk stats update' 
      }));
      
      if (typeof toast === 'function') {
        toast({
          title: 'Error',
          description: error.message || 'Failed to start optimized risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      } else if (toast.toast) {
        toast.toast({
          title: 'Error',
          description: error.message || 'Failed to start optimized risk stats update',
          status: 'error',
          duration: 5000,
          isClosable: true,
        });
      }
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
          
          if (typeof toast === 'function') {
            toast({
              title: 'Job Completed',
              description: `Processed ${result.total_records} records in ${result.duration_seconds?.toFixed(2) || '?'} seconds`,
              status: 'success',
              duration: 5000,
              isClosable: true,
            });
          } else if (toast.toast) {
            toast.toast({
              title: 'Job Completed',
              description: `Processed ${result.total_records} records in ${result.duration_seconds?.toFixed(2) || '?'} seconds`,
              status: 'success',
              duration: 5000,
              isClosable: true,
            });
          }
        } else if (result.status === 'failed') {
          if (typeof toast === 'function') {
            toast({
              title: 'Job Failed',
              description: result.error_message || 'Risk stats update failed',
              status: 'error',
              duration: 5000,
              isClosable: true,
            });
          } else if (toast.toast) {
            toast.toast({
              title: 'Job Failed',
              description: result.error_message || 'Risk stats update failed',
              status: 'error',
              duration: 5000,
              isClosable: true,
            });
          }
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
  
  // Create simple tailwind-based replacements for missing ChakraUI components
  const Stat = ({children}) => (
    <div className="flex flex-col py-2 px-3">
      {children}
    </div>
  );
  
  const StatLabel = ({children}) => (
    <div className="text-xs text-gray-600 font-semibold uppercase">
      {children}
    </div>
  );
  
  const StatNumber = ({children}) => (
    <div className="text-lg font-bold">
      {children}
    </div>
  );
  
  const VStack = ({children, spacing, align}) => (
    <div className={`flex flex-col space-y-${spacing || 4} ${align === 'stretch' ? 'w-full' : ''}`}>
      {children}
    </div>
  );
  
  const Divider = () => (
    <hr className="border-t border-gray-200 my-2" />
  );
  
  const Progress = ({value, isIndeterminate, colorScheme, size}) => {
    const colors = {
      'yellow': 'bg-yellow-500',
      'blue': 'bg-blue-500',
      'green': 'bg-green-500',
      'red': 'bg-red-500',
      'gray': 'bg-gray-500'
    };
    
    return (
      <div className="w-full bg-gray-200 rounded-full h-2.5 mb-4">
        <div 
          className={`h-2.5 rounded-full ${colors[colorScheme] || colors.blue} ${isIndeterminate ? 'progress-bar-striped progress-bar-animated' : ''}`}
          style={{width: `${value}%`}}
        ></div>
      </div>
    );
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-4">
      <div className="flex flex-col space-y-4 w-full">
        <h2 className="font-bold text-xl">Risk Statistics Management</h2>
        <hr className="border-t border-gray-200 my-2" />
        
        {/* Current stats info */}
        {statsStatus && statsStatus.has_data && (
          <div className="p-3 bg-gray-50 rounded-md">
            <div className="flex flex-wrap justify-between">
              <Stat>
                <StatLabel>Last Updated</StatLabel>
                <StatNumber>{new Date(statsStatus.latest_import_date).toLocaleDateString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Total Records</StatLabel>
                <StatNumber>{statsStatus.total_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Equity</StatLabel>
                <StatNumber>{statsStatus.equity_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Fixed Income</StatLabel>
                <StatNumber>{statsStatus.fixed_income_records.toLocaleString()}</StatNumber>
              </Stat>
              
              <Stat>
                <StatLabel>Alternatives</StatLabel>
                <StatNumber>{statsStatus.alternatives_records.toLocaleString()}</StatNumber>
              </Stat>
            </div>
            
            {statsStatus.job_completed_at && (
              <p className="mt-2 text-sm text-gray-600">
                Last job completed at {new Date(statsStatus.job_completed_at).toLocaleString()}
                {statsStatus.job_duration_seconds && ` (${statsStatus.job_duration_seconds.toFixed(2)}s)`}
              </p>
            )}
          </div>
        )}
        
        {/* Error alert */}
        {error && (
          <div className="rounded-md p-4 border bg-red-100 text-red-800 border-red-200">
            {error}
          </div>
        )}
        
        {/* Current job status */}
        {currentJob && (
          <div className="p-4 border rounded-md">
            <div className="flex justify-between mb-2">
              <p className="font-bold">Job #{currentJob.job_id}</p>
              <span className={`inline-block px-2 py-1 text-xs font-semibold rounded-full bg-${getStatusColor(currentJob.status)}-100 text-${getStatusColor(currentJob.status)}-800`}>
                {currentJob.status.toUpperCase()}
              </span>
            </div>
            
            <Progress 
              value={calculateProgress(currentJob)} 
              colorScheme={getStatusColor(currentJob.status)}
              isIndeterminate={['pending', 'running'].includes(currentJob.status)}
            />
            
            <div className="flex flex-wrap mt-3 space-x-4">
              {currentJob.total_records > 0 && (
                <Stat>
                  <StatLabel>Records</StatLabel>
                  <StatNumber>{currentJob.total_records}</StatNumber>
                </Stat>
              )}
              
              {currentJob.duration_seconds && (
                <Stat>
                  <StatLabel>Duration</StatLabel>
                  <StatNumber>{currentJob.duration_seconds.toFixed(2)}s</StatNumber>
                </Stat>
              )}
              
              {currentJob.memory_usage_mb && (
                <Stat>
                  <StatLabel>Memory</StatLabel>
                  <StatNumber>{currentJob.memory_usage_mb.toFixed(1)} MB</StatNumber>
                </Stat>
              )}
            </div>
            
            {currentJob.error_message && (
              <div className="rounded-md p-4 border bg-red-100 text-red-800 border-red-200 mt-2">
                {currentJob.error_message}
              </div>
            )}
          </div>
        )}
        
        {/* Action buttons */}
        <div className="flex justify-end space-x-4 mt-2">
          <button 
            onClick={fetchRiskStatsStatus} 
            className="px-2 py-1 text-sm border border-blue-500 text-blue-500 rounded font-semibold hover:bg-blue-50"
          >
            Refresh Status
          </button>
          
          <button 
            onClick={startOptimizedRiskStatsUpdate} 
            className="px-2 py-1 text-sm bg-green-500 hover:bg-green-600 text-white rounded font-semibold"
            disabled={isLoading || (currentJob && ['pending', 'running'].includes(currentJob.status))}
            title="High-performance optimized update of risk statistics"
          >
            {isLoading ? (
              <span>
                <span className="inline-block w-4 h-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin"></span>
                Processing...
              </span>
            ) : 'Update Risk Stats'}
          </button>
        </div>
      </div>
    </div>
  );
};

// Make component available globally
window.RiskStatsJobManager = RiskStatsJobManager;