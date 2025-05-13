/**
 * API service for nori Financial Portfolio Reporting
 */
// Using the already loaded axios from CDN
// No need to import

// Use relative URL to avoid CORS issues in different environments
const API_BASE_URL = '';

// IMPORTANT: Define api as a global variable
// This avoids the "exports is not defined" error that occurs when Babel tries to process ESM exports
/**
 * Validate and normalize chart data structure
 * Ensures consistent formatting for all chart types
 * @param {Object} data - Raw chart data from API
 * @param {string} chartType - Type of chart (allocation, liquidity, performance)
 * @returns {Object} Validated and normalized chart data
 */
function validateChartData(data, chartType) {
    console.log(`🧪 Validating ${chartType} chart data`);
    
    // Default empty structure based on chart type
    const emptyData = {
        labels: ['No Data'],
        datasets: [{ 
            data: [100], 
            backgroundColor: chartType === 'performance' 
                ? ['rgba(224, 224, 224, 0.2)']
                : ['#e0e0e0'],
            borderColor: chartType === 'performance' ? '#e0e0e0' : undefined,
            label: `No ${chartType} data available`
        }]
    };
    
    // If data is empty or not an object, return empty structure
    if (!data || typeof data !== 'object') {
        console.warn(`⚠️ ${chartType} chart data is null, undefined, or not an object`);
        return emptyData;
    }
    
    // Check for required properties
    if (!data.labels || !Array.isArray(data.labels)) {
        console.warn(`⚠️ ${chartType} chart data missing labels array`);
        return emptyData;
    }
    
    if (!data.datasets || !Array.isArray(data.datasets) || data.datasets.length === 0) {
        console.warn(`⚠️ ${chartType} chart data missing datasets array`);
        return emptyData;
    }
    
    // Check for empty labels
    if (data.labels.length === 0) {
        console.warn(`⚠️ ${chartType} chart has empty labels array`);
        return emptyData;
    }
    
    // Check if first dataset contains data array
    if (!data.datasets[0].data || !Array.isArray(data.datasets[0].data)) {
        console.warn(`⚠️ ${chartType} chart missing data array in first dataset`);
        return emptyData;
    }
    
    // Check for empty data
    if (data.datasets[0].data.length === 0) {
        console.warn(`⚠️ ${chartType} chart has empty data array`);
        return emptyData;
    }
    
    // Create a clean, normalized structure
    const normalizedData = {
        labels: [...data.labels],
        datasets: [{
            data: data.datasets[0].data.map(val => 
                val === null || val === undefined || isNaN(Number(val)) ? 0 : Number(val)
            ),
            backgroundColor: data.datasets[0].backgroundColor || 
                (chartType === 'performance' 
                    ? ['rgba(52, 152, 219, 0.2)'] 
                    : ['#3498db', '#e74c3c', '#2ecc71', '#f1c40f', '#9b59b6']),
            borderColor: chartType === 'performance' 
                ? (data.datasets[0].borderColor || '#3498db') 
                : undefined,
            label: data.datasets[0].label || `${chartType.charAt(0).toUpperCase() + chartType.slice(1)} Data`
        }]
    };
    
    // Ensure we have enough colors
    if (chartType !== 'performance' && 
        normalizedData.datasets[0].backgroundColor.length < normalizedData.datasets[0].data.length) {
        
        // Default color palette
        const defaultColors = ['#3498db', '#e74c3c', '#2ecc71', '#f1c40f', '#9b59b6', 
                              '#e67e22', '#1abc9c', '#34495e', '#7f8c8d', '#d35400'];
        
        // Copy existing colors
        const existingColors = [...normalizedData.datasets[0].backgroundColor];
        
        // Add colors from default palette until we have enough
        while (normalizedData.datasets[0].backgroundColor.length < normalizedData.datasets[0].data.length) {
            const index = normalizedData.datasets[0].backgroundColor.length % defaultColors.length;
            normalizedData.datasets[0].backgroundColor.push(defaultColors[index]);
        }
        
        console.log(`🎨 Extended colors for ${chartType} chart: `, 
            normalizedData.datasets[0].backgroundColor.length);
    }
    
    // Ensure data length and labels length match
    if (normalizedData.datasets[0].data.length !== normalizedData.labels.length) {
        console.warn(`⚠️ ${chartType} chart data/labels length mismatch: `,
            {dataLength: normalizedData.datasets[0].data.length, labelsLength: normalizedData.labels.length});
        
        // Find the minimum length
        const minLength = Math.min(
            normalizedData.datasets[0].data.length,
            normalizedData.labels.length
        );
        
        // Truncate both arrays to match
        normalizedData.datasets[0].data = normalizedData.datasets[0].data.slice(0, minLength);
        normalizedData.labels = normalizedData.labels.slice(0, minLength);
        
        // For pie charts, also truncate the colors
        if (chartType !== 'performance') {
            normalizedData.datasets[0].backgroundColor = 
                normalizedData.datasets[0].backgroundColor.slice(0, minLength);
        }
    }
    
    console.log(`✅ ${chartType} chart data successfully validated`);
    return normalizedData;
}

window.api = {
    /**
     * Check API health
     */
    checkHealth: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/health`);
            return response.data;
        } catch (error) {
            console.error('Health check error:', error);
            throw error;
        }
    },
    
    /**
     * Upload a file to the specified endpoint
     * @param {string} endpoint - API endpoint to upload to
     * @param {FormData} formData - Form data containing the file
     */
    uploadFile: async (endpoint, formData) => {
        try {
            const controller = new AbortController();
            // Set a 60 second timeout for large file uploads
            const timeoutId = setTimeout(() => controller.abort(), 60000);
            
            const response = await axios.post(`${API_BASE_URL}${endpoint}`, formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                    'Accept': 'application/json'  // Force JSON response
                },
                signal: controller.signal,
                // Show progress for large files
                onUploadProgress: (progressEvent) => {
                    const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
                    console.log(`Upload progress: ${percentCompleted}%`);
                    // You could also update UI with this progress
                }
            });
            
            clearTimeout(timeoutId);
            
            // Check if this is a background processing response
            if (response.data.success && response.data.status === 'processing' && response.data.status_url) {
                // Return the response with a flag to indicate background processing is happening
                return {
                    ...response.data,
                    isBackgroundProcessing: true
                };
            }
            
            return response.data;
        } catch (error) {
            console.error('Upload error:', error);
            // Handle abort errors specifically
            if (error.name === 'AbortError' || axios.isCancel(error)) {
                return { 
                    success: false, 
                    message: 'Upload timed out. The server is taking too long to process your file.' 
                };
            }
            
            // If we got an error response from server
            if (error.response) {
                // The server responded with a status code outside of 2xx range
                console.error('Error response data:', error.response.data);
                return { 
                    success: false, 
                    message: error.response.data.message || 'Server error during upload',
                    errors: error.response.data.errors || []
                };
            } else if (error.request) {
                // The request was made but no response was received
                return { 
                    success: false, 
                    message: 'No response received from server. Please try again.' 
                };
            } else {
                // Something happened in setting up the request
                return { 
                    success: false, 
                    message: error.message || 'An unexpected error occurred' 
                };
            }
        }
    },
    
    /**
     * Check the status of a background upload
     * @param {string} statusUrl - URL to check status
     */
    checkUploadStatus: async (statusUrl) => {
        try {
            const response = await axios.get(`${API_BASE_URL}${statusUrl}`);
            return response.data;
        } catch (error) {
            console.error('Status check error:', error);
            return { 
                success: false, 
                status: 'error',
                message: 'Error checking upload status' 
            };
        }
    },
    
    /**
     * Get ownership hierarchy tree
     */
    getOwnershipTree: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/ownership-tree`);
            
            // Check for success flag in API response
            if (response.data && response.data.success === true) {
                // API returns { success: true, data: { tree structure... } }
                return response.data.data;
            }
            
            // If response.data is directly the data without a success wrapper
            if (response.data && !response.data.success) {
                // Direct return of data
                console.log('Ownership tree data received', response.data);
                return response.data;
            }
            
            console.warn('Ownership tree data is empty or malformed', response.data);
            return [];
        } catch (error) {
            console.error('Ownership tree error:', error);
            // Return empty array instead of throwing
            throw new Error('Failed to load ownership tree: ' + (error.message || 'Unknown error'));
        }
    },
    
    /**
     * Generate a portfolio report
     * @param {string} date - Report date (YYYY-MM-DD)
     * @param {string} level - Report level (client, group, portfolio, account, custom)
     * @param {string} levelKey - Key for the selected level
     * @param {string} displayFormat - Format to display values (percent, dollar)
     */
    getPortfolioReport: async (date, level, levelKey, displayFormat = 'percent') => {
        try {
            // Set a longer timeout for this request (30 seconds)
            const response = await axios.get(
                `${API_BASE_URL}/api/portfolio-report-template`, {
                    params: { date, level, level_key: levelKey, display_format: displayFormat },
                    timeout: 30000 // 30 seconds timeout
                }
            );
            
            // Validate response data
            if (!response.data) {
                console.warn('Portfolio report data is empty');
                throw new Error('No portfolio data available');
            }
            
            // Check for timeout_occurred flag in the response
            if (response.data && response.data.timeout_occurred) {
                console.warn('Portfolio report generated with partial data due to timeout');
                // Return the partial data but mark it as having timeout issues
                return {
                    ...response.data,
                    warning: 'The report may have incomplete risk metrics due to processing timeout.'
                };
            }
            
            return response.data;
        } catch (error) {
            console.error('Portfolio report error:', error);
            
            // Create a more user-friendly error message
            let errorMessage = 'Failed to generate portfolio report.';
            
            if (error.response) {
                // Server returned an error response
                if (error.response.status === 500) {
                    errorMessage = 'The server encountered an error while processing the report. This might be due to complex data or encoding issues with security names.';
                } else if (error.response.status === 404) {
                    errorMessage = 'No data found for the selected date and portfolio.';
                } else if (error.response.data && error.response.data.error) {
                    errorMessage = error.response.data.error;
                }
            } else if (error.code === 'ECONNABORTED') {
                // Timeout error
                errorMessage = 'The report generation timed out. Try again or select a different portfolio with fewer positions.';
            } else if (!error.response) {
                // Network error
                errorMessage = 'Network error. Please check your connection and try again.';
            }
            
            // Throw enhanced error
            throw new Error(errorMessage);
        }
    },
    
    /**
     * Get asset allocation chart data
     * @param {string} date - Report date (YYYY-MM-DD)
     * @param {string} level - Report level (client, group, portfolio, account, custom)
     * @param {string} levelKey - Key for the selected level
     */
    getAllocationChartData: async (date, level, levelKey) => {
        try {
            console.log(`🔍 Fetching allocation chart data: date=${date}, level=${level}, key=${levelKey}`);
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/allocation`, {
                    params: { date, level, level_key: levelKey }
                }
            );
            
            console.log('📊 Raw allocation chart data received:', JSON.stringify(response.data));
            
            // Validate and normalize response data
            const validatedData = validateChartData(response.data, 'allocation');
            
            return validatedData;
        } catch (error) {
            console.error('❌ Allocation chart error:', error);
            // Return a default structure so UI doesn't break
            return {
                labels: ['Error'],
                datasets: [{ 
                    data: [100], 
                    backgroundColor: ['#ffebee'],
                    label: 'Error loading data'
                }]
            };
        }
    },
    
    /**
     * Get liquidity chart data
     * @param {string} date - Report date (YYYY-MM-DD)
     * @param {string} level - Report level (client, group, portfolio, account, custom)
     * @param {string} levelKey - Key for the selected level
     */
    getLiquidityChartData: async (date, level, levelKey) => {
        try {
            console.log(`🔍 Fetching liquidity chart data: date=${date}, level=${level}, key=${levelKey}`);
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/liquidity`, {
                    params: { date, level, level_key: levelKey }
                }
            );
            
            console.log('💧 Raw liquidity chart data received:', JSON.stringify(response.data));
            
            // Validate and normalize response data
            const validatedData = validateChartData(response.data, 'liquidity');
            
            return validatedData;
        } catch (error) {
            console.error('❌ Liquidity chart error:', error);
            // Return a default structure so UI doesn't break
            return {
                labels: ['Error'],
                datasets: [{ 
                    data: [100], 
                    backgroundColor: ['#ffebee'],
                    label: 'Error loading data'
                }]
            };
        }
    },
    
    /**
     * Get performance chart data
     * @param {string} date - Report date (YYYY-MM-DD)
     * @param {string} level - Report level (client, group, portfolio, account, custom)
     * @param {string} levelKey - Key for the selected level
     * @param {string} period - Performance period (1D, MTD, QTD, YTD)
     */
    getPerformanceChartData: async (date, level, levelKey, period = 'YTD') => {
        try {
            console.log(`📊 Fetching performance chart data: date=${date}, level=${level}, key=${levelKey}, period=${period}`);
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/performance`, {
                    params: { date, level, level_key: levelKey, period }
                }
            );
            
            console.log('📈 Raw performance chart data received:', JSON.stringify(response.data));
            
            // Validate and normalize response data
            const validatedData = validateChartData(response.data, 'performance');
            
            return validatedData;
        } catch (error) {
            console.error('❌ Performance chart error:', error);
            // Return a default structure so UI doesn't break
            return {
                labels: ['Error'],
                datasets: [{ 
                    data: [0],
                    label: 'Error loading performance data',
                    borderColor: '#f44336',
                    backgroundColor: 'rgba(244, 67, 54, 0.2)'
                }]
            };
        }
    },
    
    /**
     * Get entity options (clients, portfolios, accounts)
     * @param {string} type - Entity type (client, portfolio, account)
     */
    getEntityOptions: async (type = 'client') => {
        try {
            const response = await axios.get(
                `${API_BASE_URL}/api/entity-options`, {
                    params: { type }
                }
            );
            // Log the full response for debugging
            console.log(`Entity options response for ${type}:`, response.data);
            
            // The API returns data in the "options" field, not "data"
            return response.data.options || [];
        } catch (error) {
            console.error('Entity options error:', error);
            return [];
        }
    },
    
    /**
     * Get all model portfolios
     */
    getModelPortfolios: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/model-portfolios`);
            return response.data.portfolios || [];
        } catch (error) {
            console.error('Error fetching model portfolios:', error);
            return [];
        }
    },
    
    /**
     * Get a specific model portfolio by ID
     * @param {number} id - The model portfolio ID
     */
    getModelPortfolioDetail: async (id) => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/model-portfolios/${id}`);
            return response.data.portfolio || null;
        } catch (error) {
            console.error(`Error fetching model portfolio ${id}:`, error);
            return null;
        }
    },
    
    /**
     * Compare a portfolio against a model portfolio
     * @param {string} portfolioId - The portfolio to compare
     * @param {number} modelId - The model portfolio ID
     * @param {string} date - The date for comparison
     */
    compareWithModel: async (portfolioId, modelId, date) => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/compare-portfolio`, {
                params: { portfolio_id: portfolioId, model_id: modelId, date }
            });
            return response.data.comparison || null;
        } catch (error) {
            console.error('Error comparing portfolio with model:', error);
            throw error;
        }
    },
    
    /**
     * Get portfolio risk metrics
     * @param {string} date - Report date (YYYY-MM-DD)
     * @param {string} level - Level for the report (client, portfolio, account)
     * @param {string} levelKey - Key for the selected level
     * @param {number} sampleSize - Optional sample size for large portfolios (for performance)
     */
    getPortfolioRiskMetrics: async (date, level, levelKey, sampleSize = null) => {
        try {
            const params = { 
                date, 
                level, 
                level_key: levelKey 
            };
            
            // Add sample_size parameter for large portfolios if provided
            if (sampleSize && Number.isInteger(sampleSize) && sampleSize > 0) {
                params.sample_size = sampleSize;
            }
            
            const response = await axios.get(
                `${API_BASE_URL}/api/portfolio/risk-metrics`,
                { params }
            );
            
            if (!response.data || response.data.error) {
                console.warn('Portfolio risk metrics data error:', response.data?.error);
                throw new Error(response.data?.error || 'Failed to fetch portfolio risk metrics');
            }
            
            return response.data;
        } catch (error) {
            console.error('Portfolio risk metrics error:', error);
            throw new Error(error.response?.data?.error || 'Failed to fetch portfolio risk metrics');
        }
    },
    
    /**
     * Get risk statistics status information
     * Returns information about the last update and available records
     */
    /**
     * Start an asynchronous risk statistics update job
     * @param {boolean} useTestFile - Whether to use a test file instead of downloading from Egnyte
     * @param {boolean} debugMode - Enable debug mode for detailed logging
     * @returns {Object} Job information including the job ID and status
     */
    updateRiskStats: async (useTestFile = false, debugMode = false) => {
        try {
            const params = {};
            if (useTestFile) params.use_test_file = 'true';
            if (debugMode) params.debug = 'true';
            
            const response = await axios.post(`${API_BASE_URL}/api/risk-stats/update`, null, { params });
            return response.data;
        } catch (error) {
            console.error('Error starting risk stats update:', error);
            return {
                success: false,
                error: error.response?.data?.error || error.message || 'Failed to start risk statistics update'
            };
        }
    },

    /**
     * High-performance direct risk statistics update (optimized version)
     * This function uses the optimized endpoint that completes in 2-3 seconds.
     * 
     * @param {boolean} useTestFile - Whether to use a test file instead of downloading from Egnyte
     * @param {boolean} debugMode - Enable debug mode for detailed logging
     * @param {number} batchSize - Size of batches for database operations (default: 1000)
     * @param {number} workers - Number of parallel workers (default: 3)
     * @returns {Object} Processing results including timing and counts
     */
    /**
     * Trigger precalculation of risk metrics for all entities
     * This helps improve performance by having metrics calculated ahead of time
     * 
     * @param {string} date - Optional date to calculate risk metrics for (YYYY-MM-DD format)
     * @returns {Object} Response with status of the precalculation request
     */
    triggerPrecalculation: async (date = null) => {
        try {
            const params = {};
            if (date) params.date = date;
            
            const response = await axios.post(`${API_BASE_URL}/api/precalculate`, null, { params });
            return response.data;
        } catch (error) {
            console.error('Error triggering precalculation:', error);
            return {
                success: false,
                error: error.response?.data?.error || error.message || 'Failed to trigger precalculation'
            };
        }
    },
    
    updateRiskStatsOptimized: async (useTestFile = false, debugMode = false, batchSize = 1000, workers = 3) => {
        try {
            const params = {};
            if (useTestFile) params.use_test_file = 'true';
            if (debugMode) params.debug = 'true';
            if (batchSize) params.batch_size = batchSize;
            if (workers) params.workers = workers;
            
            const response = await axios.post(`${API_BASE_URL}/api/risk-stats/update-optimized`, null, { params });
            return response.data;
        } catch (error) {
            console.error('Error starting optimized risk stats update:', error);
            return {
                success: false,
                error: error.response?.data?.error || error.message || 'Failed to start optimized risk statistics update'
            };
        }
    },
    
    /**
     * Update risk statistics with high-performance turbo implementation
     * This is the most efficient implementation, designed to meet the 2-3 second target
     * 
     * @param {boolean} useTestFile - Whether to use a test file instead of downloading from Egnyte
     * @param {boolean} debugMode - Enable debug mode for detailed logging
     * @param {number} batchSize - Size of batches for database operations (default: 1000)
     * @param {number} workers - Number of parallel worker threads (default: 3)
     * @returns {Object} Processing results including timing and record counts
     */
    updateRiskStatsTurbo: async (useTestFile = false, debugMode = false, batchSize = 1000, workers = 3) => {
        try {
            const params = {};
            if (useTestFile) params.use_test_file = 'true';
            if (debugMode) params.debug = 'true';
            if (batchSize) params.batch_size = batchSize;
            if (workers) params.workers = workers;
            
            const response = await axios.post(`${API_BASE_URL}/api/risk-stats/update-turbo`, null, { params });
            return response.data;
        } catch (error) {
            console.error('Error starting turbo risk stats update:', error);
            return {
                success: false,
                error: error.response?.data?.error || error.message || 'Failed to start turbo risk statistics update'
            };
        }
    },
    
    /**
     * Get the status of a risk statistics update job
     * @param {number} jobId - The ID of the job to check
     * @returns {Object} Job status and details
     */
    getRiskStatsJobStatus: async (jobId) => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/risk-stats/jobs/${jobId}`);
            return response.data;
        } catch (error) {
            console.error(`Error getting risk stats job status (ID: ${jobId}):`, error);
            return {
                success: false,
                error: error.response?.data?.error || error.message || 'Failed to get job status'
            };
        }
    },
    
    /**
     * Get risk statistics status information
     * Returns information about the last update and available records
     */
    getRiskStatsStatus: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/risk-stats/status`);
            return response.data;
        } catch (error) {
            console.error('Error fetching risk stats status:', error);
            return {
                success: false,
                error: error.message || 'Failed to fetch risk statistics status'
            };
        }
    },
    
    /**
     * Get risk statistics data with optional filters
     * @param {string} assetClass - Optional filter by asset class (Equity, Fixed Income, Alternatives)
     * @param {string} secondLevel - Optional filter by second level category
     * @param {string} position - Optional filter by position/security name
     * @param {string} ticker - Optional filter by ticker symbol
     */
    getRiskStats: async (assetClass = null, secondLevel = null, position = null, ticker = null) => {
        try {
            const params = {};
            if (assetClass) params.asset_class = assetClass;
            if (secondLevel) params.second_level = secondLevel;
            if (position) params.position = position;
            if (ticker) params.ticker = ticker;
            
            const response = await axios.get(`${API_BASE_URL}/api/risk-stats`, { params });
            return response.data;
        } catch (error) {
            console.error('Error fetching risk stats:', error);
            return {
                success: false,
                error: error.message || 'Failed to fetch risk statistics'
            };
        }
    },
    
    /**
     * Get a list of securities that don't have matching risk statistics
     * This is useful for identifying which securities need risk data to be uploaded
     */
    getUnmatchedSecurities: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/risk-stats/unmatched`);
            return response.data;
        } catch (error) {
            console.error('Error fetching unmatched securities:', error);
            return {
                success: false,
                error: error.message || 'Failed to fetch unmatched securities list'
            };
        }
    },
    
    // Legacy updateRiskStats function replaced by the implementation above
    
    /**
     * Get metadata options for entity classifications, etc.
     */
    getMetadataOptions: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/metadata-options`);
            return response.data;
        } catch (error) {
            console.error('Error fetching metadata options:', error);
            return {
                success: false,
                error: error.message || 'Failed to fetch metadata options'
            };
        }
    }
};
