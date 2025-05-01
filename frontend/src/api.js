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
            const response = await axios.post(`${API_BASE_URL}${endpoint}`, formData, {
                headers: {
                    'Content-Type': 'multipart/form-data'
                }
            });
            return response.data;
        } catch (error) {
            console.error('Upload error:', error);
            throw error;
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
