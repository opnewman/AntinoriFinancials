/**
 * API service for ANTINORI Financial Portfolio Reporting
 */
// Using the already loaded axios from CDN
// No need to import

// Use relative URL to avoid CORS issues in different environments
const API_BASE_URL = '';

// Define api as a window global to avoid "exports is not defined" error
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
     */
    getPortfolioReport: async (date, level, levelKey) => {
        try {
            const response = await axios.get(
                `${API_BASE_URL}/api/portfolio-report`, {
                    params: { date, level, level_key: levelKey }
                }
            );
            
            // Validate response data
            if (!response.data) {
                console.warn('Portfolio report data is empty');
                throw new Error('No portfolio data available');
            }
            
            return response.data;
        } catch (error) {
            console.error('Portfolio report error:', error);
            // Let the component handle this error
            throw new Error(error.response?.data?.detail || 'Failed to generate portfolio report');
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
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/allocation`, {
                    params: { date, level, level_key: levelKey }
                }
            );
            
            if (!response.data || !response.data.labels || !response.data.datasets) {
                console.warn('Allocation chart data is incomplete');
                return {
                    labels: ['No Data'],
                    datasets: [{ 
                        data: [100], 
                        backgroundColor: ['#e0e0e0'],
                        label: 'No allocation data available'
                    }]
                };
            }
            
            return response.data;
        } catch (error) {
            console.error('Allocation chart error:', error);
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
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/liquidity`, {
                    params: { date, level, level_key: levelKey }
                }
            );
            
            if (!response.data || !response.data.labels || !response.data.datasets) {
                console.warn('Liquidity chart data is incomplete');
                return {
                    labels: ['No Data'],
                    datasets: [{ 
                        data: [100], 
                        backgroundColor: ['#e0e0e0'],
                        label: 'No liquidity data available'
                    }]
                };
            }
            
            return response.data;
        } catch (error) {
            console.error('Liquidity chart error:', error);
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
            const response = await axios.get(
                `${API_BASE_URL}/api/charts/performance`, {
                    params: { date, level, level_key: levelKey, period }
                }
            );
            
            if (!response.data || !response.data.labels || !response.data.datasets) {
                console.warn('Performance chart data is incomplete');
                return {
                    labels: ['No Data'],
                    datasets: [{ 
                        data: [0],
                        label: 'No performance data available',
                        borderColor: '#e0e0e0',
                        backgroundColor: 'rgba(224, 224, 224, 0.2)'
                    }]
                };
            }
            
            return response.data;
        } catch (error) {
            console.error('Performance chart error:', error);
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
            return response.data.data || [];
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
    }
};
