/**
 * API service for ANTINORI Financial Portfolio Reporting
 */
// Using the already loaded axios from CDN
// No need to import

// Use relative URL to avoid CORS issues in different environments
const API_BASE_URL = '';

const api = {
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
     * Get ownership hierarchy tree
     */
    getOwnershipTree: async () => {
        try {
            const response = await axios.get(`${API_BASE_URL}/api/ownership-tree`);
            return response.data;
        } catch (error) {
            console.error('Ownership tree error:', error);
            throw error;
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
            return response.data;
        } catch (error) {
            console.error('Portfolio report error:', error);
            throw error;
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
            return response.data;
        } catch (error) {
            console.error('Allocation chart error:', error);
            throw error;
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
            return response.data;
        } catch (error) {
            console.error('Liquidity chart error:', error);
            throw error;
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
            return response.data;
        } catch (error) {
            console.error('Performance chart error:', error);
            throw error;
        }
    }
};
