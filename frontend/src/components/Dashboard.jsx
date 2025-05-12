// Import chart components
const { PieChart, DoughnutChart, LineChart } = window.Charts || {};

class Dashboard extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            // Use 2025-05-01 as the specific date we know data exists for
            reportDate: '2025-05-01',
            reportLevel: 'client',
            levelKey: '',
            levelOptions: [],
            reportData: null,
            allocationsChart: null,
            liquidityChart: null,
            performanceChart: null,
            error: '',
            loading: false,
            displayFormat: 'percent' // Default to percentage display
        };
        this._isMounted = false; // Track component mount state
    }
    
    componentDidMount() {
        this._isMounted = true;
        this.fetchEntityOptions();
    }
    
    componentWillUnmount() {
        this._isMounted = false;
    }
    
    // Fetch entity options on component mount
    fetchEntityOptions = async () => {
        try {
            console.log('Fetching client options...');
            // Use window.api instead of api to access the global API object 
            const entityOptions = await window.api.getEntityOptions('client');
            console.log('Received entity options:', entityOptions);
            
            // Make sure component is still mounted before updating state
            if (!this._isMounted) return;
            
            if (entityOptions && entityOptions.length > 0) {
                // entityOptions is an array of objects with key and display properties
                const formattedOptions = entityOptions.map(entity => ({
                    value: entity.key,
                    label: entity.display
                }));
                
                this.setState({ levelOptions: formattedOptions });
                console.log('Set formatted options:', formattedOptions);
                
                // Set default to "All Clients" if it exists, otherwise first client
                const allClientsOption = formattedOptions.find(opt => opt.value === 'All Clients');
                if (allClientsOption) {
                    this.setState({ levelKey: allClientsOption.value });
                    console.log('Set level key to All Clients');
                } else if (formattedOptions.length > 0) {
                    this.setState({ levelKey: formattedOptions[0].value });
                    console.log('Set level key to first option:', formattedOptions[0].value);
                }
            }
        } catch (err) {
            console.error('Error fetching initial data:', err);
            if (this._isMounted) {
                this.setState({ error: 'Failed to load entity options. Please try again later.' });
            }
        }
    };
    
    // Update entity options when report level changes
    updateLevelOptions = async (newLevel) => {
        try {
            console.log(`Fetching options for level: ${newLevel}`);
            const options = await window.api.getEntityOptions(newLevel);
            console.log(`Received ${options.length} options for level ${newLevel}`);
            
            // Only update state if component is still mounted
            if (!this._isMounted) return;
            
            // options is an array of objects with key and display properties
            const formattedOptions = options.map(opt => ({
                value: opt.key,
                label: opt.display
            }));
            
            this.setState({ 
                levelOptions: formattedOptions,
                levelKey: formattedOptions.length > 0 ? formattedOptions[0].value : ''
            });
            console.log('Set level options:', formattedOptions.slice(0, 5)); // Log only first 5 for brevity
        } catch (err) {
            console.error(`Error fetching ${newLevel} options:`, err);
            if (this._isMounted) {
                this.setState({ error: `Failed to load ${newLevel} options. Please try again later.` });
            }
        }
    };
    
    // Handle report level change
    handleLevelChange = (e) => {
        const newLevel = e.target.value;
        this.setState({ 
            reportLevel: newLevel,
            levelKey: '', // Reset the level key when changing report level
            levelOptions: [] // Clear options until new ones are loaded
        });
        this.updateLevelOptions(newLevel);
    };
    
    // Generate report
    generateReport = async () => {
        const { reportDate, reportLevel, levelKey, displayFormat } = this.state;
        
        console.log("📊 Generating report with params:", { reportDate, reportLevel, levelKey, displayFormat });
        
        if (!levelKey) {
            console.warn("❌ Report generation aborted - no levelKey selected");
            this.setState({ error: 'Please select a valid option' });
            return;
        }
        
        // Clear previous data to avoid stale chart instances
        this.setState({
            loading: true,
            error: '',
            reportData: null,
            allocationsChart: null,
            liquidityChart: null,
            performanceChart: null
        });
        
        // Log the API endpoints being called
        console.log(`🔄 Calling API endpoints for level=${reportLevel}, key=${levelKey}, date=${reportDate}, format=${displayFormat}`);
        
        // Create all API request promises but handle them individually
        // Use window.api to access the global API object
        const portfolioReportPromise = window.api.getPortfolioReport(reportDate, reportLevel, levelKey, displayFormat);
        const allocationChartPromise = window.api.getAllocationChartData(reportDate, reportLevel, levelKey);
        const liquidityChartPromise = window.api.getLiquidityChartData(reportDate, reportLevel, levelKey);
        const performanceChartPromise = window.api.getPerformanceChartData(reportDate, reportLevel, levelKey, 'YTD');
        
        try {
            // Get portfolio report
            let hasFatalError = false;
            
            try {
                console.log("⏳ Waiting for portfolio report data...");
                const report = await portfolioReportPromise;
                console.log("✅ Portfolio report data received:", report ? "Data received" : "No data");
                
                if (report) {
                    // Check if critical sections exist
                    console.log("📝 Report data structure check:", {
                        hasAssetAllocation: !!report.asset_allocation,
                        hasEquity: !!(report.equity && report.equity.total_pct),
                        hasFixedIncome: !!(report.fixed_income && report.fixed_income.total_pct),
                        hasHardCurrency: !!(report.hard_currency && report.hard_currency.total_pct),
                        hasAlternatives: !!(report.alternatives && report.alternatives.total_pct),
                        hasRiskMetrics: !!report.risk_metrics
                    });
                }
                
                if (this._isMounted) {
                    this.setState({ reportData: report });
                }
            } catch (err) {
                console.error('❌ Portfolio report error:', err);
                if (this._isMounted) {
                    this.setState({ error: err.message || 'Failed to load portfolio report' });
                }
                hasFatalError = true;
            }
            
            // Process other chart data only if component is still mounted
            if (this._isMounted && !hasFatalError) {
                // Get allocation chart data
                try {
                    const allocations = await allocationChartPromise;
                    if (this._isMounted) {
                        this.setState({
                            allocationsChart: {
                                labels: allocations.labels,
                                datasets: [{
                                    data: allocations.datasets[0].data,
                                    backgroundColor: allocations.datasets[0].backgroundColor,
                                    borderWidth: 1,
                                    borderColor: '#fff'
                                }]
                            }
                        });
                    }
                } catch (err) {
                    console.error('Allocation chart error:', err);
                }
                
                // Get liquidity chart data
                try {
                    const liquidity = await liquidityChartPromise;
                    if (this._isMounted) {
                        this.setState({
                            liquidityChart: {
                                labels: liquidity.labels,
                                datasets: [{
                                    data: liquidity.datasets[0].data,
                                    backgroundColor: liquidity.datasets[0].backgroundColor,
                                    borderWidth: 1,
                                    borderColor: '#fff'
                                }]
                            }
                        });
                    }
                } catch (err) {
                    console.error('Liquidity chart error:', err);
                }
                
                // Get performance chart data
                try {
                    const performance = await performanceChartPromise;
                    if (this._isMounted) {
                        this.setState({
                            performanceChart: {
                                labels: performance.labels,
                                datasets: performance.datasets
                            }
                        });
                    }
                } catch (err) {
                    console.error('Performance chart error:', err);
                }
            }
            
            console.log('Report data generation complete');
            
        } catch (err) {
            console.error('Error in overall report generation process:', err);
            if (this._isMounted && !this.state.error) {
                this.setState({ error: 'Failed to generate complete report. Some data may be missing.' });
            }
        } finally {
            if (this._isMounted) {
                this.setState({ loading: false });
            }
        }
    };
    
    // Format data for tables
    formatAssetAllocation = () => {
        const { reportData } = this.state;
        if (!reportData || !reportData.asset_allocation) return [];
        
        return Object.entries(reportData.asset_allocation).map(([asset, percentage]) => ({
            name: asset,
            percentage
        }));
    };
    
    formatLiquidity = () => {
        const { reportData } = this.state;
        if (!reportData || !reportData.liquidity) return [];
        
        return Object.entries(reportData.liquidity).map(([type, percentage]) => ({
            name: type,
            percentage
        }));
    };
    
    formatPerformance = () => {
        const { reportData } = this.state;
        const defaultPerformance = [
            { name: "YTD", value: 0, percentage: 0 },
            { name: "QTD", value: 0, percentage: 0 },
            { name: "MTD", value: 0, percentage: 0 }
        ];
        
        if (!reportData) {
            console.log("⚠️ formatPerformance: No report data available");
            return defaultPerformance;
        }
        
        if (!reportData.performance) {
            console.log("⚠️ formatPerformance: No performance data in report");
            return defaultPerformance;
        }
        
        try {
            console.log("🔍 Performance data type:", typeof reportData.performance);
            console.log("🔍 Performance data:", JSON.stringify(reportData.performance, null, 2));
            
            // Handle case where performance is an object rather than an array
            if (!Array.isArray(reportData.performance)) {
                if (reportData.performance === null) {
                    console.warn("⚠️ Performance data is null");
                    return defaultPerformance;
                }
                
                if (typeof reportData.performance === 'object') {
                    console.log("📊 Converting performance object to array format");
                    
                    // Convert the object to an array of objects
                    return Object.entries(reportData.performance)
                        .filter(([_, value]) => value !== null) // Skip null values
                        .map(([period, value]) => {
                            // Safely parse the value
                            let numValue = 0;
                            try {
                                if (typeof value === 'number') {
                                    numValue = value;
                                } else if (typeof value === 'string') {
                                    numValue = parseFloat(value) || 0;
                                }
                            } catch (err) {
                                console.warn(`⚠️ Could not parse performance value for ${period}:`, value);
                            }
                            
                            return {
                                name: period,
                                value: numValue,
                                percentage: numValue
                            };
                        });
                } else {
                    // It's neither an array nor an object - use default
                    console.warn("⚠️ Performance data is not an array or object:", reportData.performance);
                    return defaultPerformance;
                }
            }
            
            // If it's already an array, use the original format but with safer parsing
            console.log("📊 Processing performance array format");
            return reportData.performance
                .filter(perf => perf !== null) // Skip null entries
                .map(perf => {
                    if (!perf || typeof perf !== 'object') {
                        console.warn("⚠️ Invalid performance entry:", perf);
                        return { name: "Unknown", value: 0, percentage: 0 };
                    }
                    
                    // Safely parse values
                    let value = 0;
                    let percentage = 0;
                    
                    try {
                        if (perf.value !== undefined) {
                            value = typeof perf.value === 'number' ? perf.value : (parseFloat(perf.value) || 0);
                        }
                        
                        if (perf.percentage !== undefined) {
                            percentage = typeof perf.percentage === 'number' ? perf.percentage : (parseFloat(perf.percentage) || 0);
                        } else {
                            // If percentage is missing but value exists, use value for percentage
                            percentage = value;
                        }
                    } catch (err) {
                        console.warn(`⚠️ Error parsing performance values:`, err);
                    }
                    
                    return {
                        name: perf.period || "Period",
                        value: value,
                        percentage: percentage
                    };
                });
        } catch (error) {
            console.error("❌ Error in formatPerformance:", error);
            return defaultPerformance;
        }
    };
    
    formatRiskMetrics = () => {
        const { reportData } = this.state;
        console.log("DEBUGGING DASHBOARD: formatRiskMetrics called with reportData:", reportData);
        
        if (!reportData) {
            console.warn("DEBUGGING DASHBOARD: reportData is null or undefined");
            return [];
        }
        
        if (!reportData.risk_metrics) {
            console.warn("DEBUGGING DASHBOARD: reportData.risk_metrics is null or undefined");
            return [];
        }
        
        console.log("DEBUGGING DASHBOARD: risk_metrics structure:", JSON.stringify(reportData.risk_metrics, null, 2));
        
        // Convert the new risk_metrics object structure to an array format for display
        const riskMetricsArray = [];
        
        // Extract equity beta
        if (reportData.risk_metrics.equity && reportData.risk_metrics.equity.beta !== undefined) {
            let betaValue = typeof reportData.risk_metrics.equity.beta === 'object' 
                ? reportData.risk_metrics.equity.beta.value 
                : reportData.risk_metrics.equity.beta;
            
            riskMetricsArray.push({
                name: "Equity Beta",
                value: parseFloat(betaValue) || 0
            });
        }
        
        // Extract equity volatility
        if (reportData.risk_metrics.equity && reportData.risk_metrics.equity.volatility !== undefined) {
            let volatilityValue = typeof reportData.risk_metrics.equity.volatility === 'object' 
                ? reportData.risk_metrics.equity.volatility.value 
                : reportData.risk_metrics.equity.volatility;
                
            riskMetricsArray.push({
                name: "Equity Volatility",
                value: parseFloat(volatilityValue) || 0
            });
        }
        
        // Extract fixed income duration
        if (reportData.risk_metrics.fixed_income && reportData.risk_metrics.fixed_income.duration !== undefined) {
            let durationValue = typeof reportData.risk_metrics.fixed_income.duration === 'object' 
                ? reportData.risk_metrics.fixed_income.duration.value 
                : reportData.risk_metrics.fixed_income.duration;
                
            riskMetricsArray.push({
                name: "Fixed Income Duration",
                value: parseFloat(durationValue) || 0
            });
        }
        
        // Extract hard currency beta
        if (reportData.risk_metrics.hard_currency && reportData.risk_metrics.hard_currency.beta !== undefined) {
            let hcBetaValue = typeof reportData.risk_metrics.hard_currency.beta === 'object' 
                ? reportData.risk_metrics.hard_currency.beta.value 
                : reportData.risk_metrics.hard_currency.beta;
                
            riskMetricsArray.push({
                name: "Hard Currency Beta",
                value: parseFloat(hcBetaValue) || 0
            });
        }
        
        // Extract portfolio beta
        if (reportData.risk_metrics.portfolio && reportData.risk_metrics.portfolio.beta !== undefined) {
            let portfolioBetaValue = typeof reportData.risk_metrics.portfolio.beta === 'object' 
                ? reportData.risk_metrics.portfolio.beta.value 
                : reportData.risk_metrics.portfolio.beta;
                
            riskMetricsArray.push({
                name: "Portfolio Beta",
                value: parseFloat(portfolioBetaValue) || 0
            });
        }
        
        return riskMetricsArray;
    };
    
    // Handle display format change
    handleDisplayFormatChange = (e) => {
        const format = e.target.value;
        this.setState({ displayFormat: format }, () => {
            // Regenerate the report if we already have data
            if (this.state.reportData) {
                this.generateReport();
            }
        });
    };
    
    handleDisplayFormatToggle = () => {
        // Toggle between percent and dollar display formats
        const newFormat = this.state.displayFormat === 'percent' ? 'dollar' : 'percent';
        this.setState({ displayFormat: newFormat }, () => {
            // Regenerate the report if we already have data
            if (this.state.reportData) {
                this.generateReport();
            }
        });
    };
    
    render() {
        const { 
            reportDate, reportLevel, levelKey, levelOptions, error, loading,
            reportData, allocationsChart, liquidityChart, performanceChart,
            displayFormat
        } = this.state;
        
        if (loading && !reportData) {
            return (
                <div className="container mx-auto p-4">
                    <div className="flex items-center justify-center min-h-screen">
                        <div className="text-center">
                            <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                            <p className="mt-4 text-lg font-medium text-gray-700">Loading dashboard...</p>
                        </div>
                    </div>
                </div>
            );
        }
        
        return (
            <div className="container mx-auto p-4">
                <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                    <div className="flex justify-between items-center mb-4">
                        <h2 className="text-xl font-bold">Portfolio Dashboard</h2>
                        
                        <div className="flex space-x-2">
                            <button
                                onClick={this.handleDisplayFormatToggle}
                                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none"
                            >
                                <i className={displayFormat === 'percent' ? 'fas fa-dollar-sign mr-2' : 'fas fa-percentage mr-2'}></i>
                                {displayFormat === 'percent' ? 'Show Dollar Values' : 'Show Percentages'}
                            </button>
                            
                            <button
                                onClick={this.generateReport}
                                disabled={loading || !levelKey}
                                className={`px-4 py-2 rounded focus:outline-none ${loading || !levelKey ? 'bg-gray-400 cursor-not-allowed' : 'bg-green-800 text-white hover:bg-green-700'}`}
                            >
                                {loading ? (
                                    <span className="flex items-center">
                                        <i className="fas fa-circle-notch fa-spin mr-2"></i>
                                        Refreshing...
                                    </span>
                                ) : (
                                    <span className="flex items-center">
                                        <i className="fas fa-sync-alt mr-2"></i>
                                        Refresh Data
                                    </span>
                                )}
                            </button>
                        </div>
                    </div>
                    
                    {error && (
                        <div className="bg-red-50 text-red-600 p-3 rounded-md mb-4">
                            <i className="fas fa-exclamation-circle mr-2"></i>
                            {error}
                        </div>
                    )}
                    
                    {reportData && reportData.warning && (
                        <div className="bg-yellow-50 text-yellow-700 p-3 rounded-md mb-4">
                            <i className="fas fa-exclamation-triangle mr-2"></i>
                            {reportData.warning}
                        </div>
                    )}
                    
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                        <div className="bg-green-50 p-4 rounded-lg shadow-sm border border-green-100">
                            <h3 className="text-sm text-green-800 font-medium mb-2">
                                <i className="fas fa-calendar-day mr-2"></i>
                                Report Date
                            </h3>
                            <p className="text-xl font-bold">{new Date(reportDate).toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'})}</p>
                            <div className="mt-2">
                                <select
                                    id="reportLevel"
                                    value={reportLevel}
                                    onChange={this.handleLevelChange}
                                    className="w-full px-2 py-1 text-sm border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                                >
                                    <option value="client">Client View</option>
                                    <option value="portfolio">Portfolio View</option>
                                    <option value="group">Group View</option>
                                    <option value="account">Account View</option>
                                </select>
                            </div>
                        </div>
                        
                        <div className="bg-blue-50 p-4 rounded-lg shadow-sm border border-blue-100">
                            <h3 className="text-sm text-blue-800 font-medium mb-2">
                                <i className="fas fa-money-bill-wave mr-2"></i>
                                Total Value
                            </h3>
                            <p className="text-xl font-bold">
                                {reportData?.total_value ? new Intl.NumberFormat('en-US', {
                                    style: 'currency',
                                    currency: 'USD',
                                    minimumFractionDigits: 0,
                                    maximumFractionDigits: 0
                                }).format(reportData.total_value) : 'N/A'}
                            </p>
                            <div className="mt-2">
                                <select
                                    id="levelKey"
                                    value={levelKey}
                                    onChange={(e) => this.setState({ levelKey: e.target.value }, this.generateReport)}
                                    className="w-full px-2 py-1 text-sm border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                                >
                                    <option value="">Select...</option>
                                    {levelOptions.map((option) => (
                                        <option key={option.value} value={option.value}>
                                            {option.label}
                                        </option>
                                    ))}
                                </select>
                            </div>
                        </div>
                        
                        <div className="bg-purple-50 p-4 rounded-lg shadow-sm border border-purple-100">
                            <h3 className="text-sm text-purple-800 font-medium mb-2">
                                <i className="fas fa-chart-line mr-2"></i>
                                YTD Performance
                            </h3>
                            <p className={`text-xl font-bold ${reportData?.performance?.YTD > 0 ? 'text-green-600' : reportData?.performance?.YTD < 0 ? 'text-red-600' : ''}`}>
                                {reportData?.performance?.YTD !== undefined ? 
                                    (displayFormat === 'percent' ? reportData.performance.YTD.toFixed(2) + '%' : 
                                        new Intl.NumberFormat('en-US', {
                                            style: 'currency',
                                            currency: 'USD',
                                            minimumFractionDigits: 0,
                                            maximumFractionDigits: 0
                                        }).format(reportData.performance.YTD * reportData.total_value / 100)) 
                                    : 'N/A'}
                            </p>
                            <div className="mt-2 grid grid-cols-3 gap-1 text-xs">
                                <div className={`p-1 text-center rounded ${reportData?.performance?.MTD > 0 ? 'bg-green-100 text-green-800' : reportData?.performance?.MTD < 0 ? 'bg-red-100 text-red-800' : 'bg-gray-100 text-gray-800'}`}>
                                    MTD: {reportData?.performance?.MTD?.toFixed(2)}%
                                </div>
                                <div className={`p-1 text-center rounded ${reportData?.performance?.QTD > 0 ? 'bg-green-100 text-green-800' : reportData?.performance?.QTD < 0 ? 'bg-red-100 text-red-800' : 'bg-gray-100 text-gray-800'}`}>
                                    QTD: {reportData?.performance?.QTD?.toFixed(2)}%
                                </div>
                                <div className={`p-1 text-center rounded ${reportData?.performance?.['1D'] > 0 ? 'bg-green-100 text-green-800' : reportData?.performance?.['1D'] < 0 ? 'bg-red-100 text-red-800' : 'bg-gray-100 text-gray-800'}`}>
                                    1D: {reportData?.performance?.['1D']?.toFixed(2)}%
                                </div>
                            </div>
                        </div>
                        
                        <div className="bg-yellow-50 p-4 rounded-lg shadow-sm border border-yellow-100">
                            <h3 className="text-sm text-yellow-800 font-medium mb-2">
                                <i className="fas fa-tint mr-2"></i>
                                Liquidity
                            </h3>
                            <div className="flex justify-between items-center">
                                <p className="text-xl font-bold">
                                    {reportData?.liquidity?.Liquid ? reportData.liquidity.Liquid.toFixed(1) + '%' : 'N/A'}
                                </p>
                                <p className="text-sm text-gray-600">Liquid</p>
                            </div>
                            <div className="w-full bg-gray-200 rounded-full h-2.5 mt-2 mb-4">
                                <div className="bg-yellow-600 h-2.5 rounded-full" style={{ width: `${reportData?.liquidity?.Liquid || 0}%` }}></div>
                            </div>
                            <div className="text-xs text-gray-500 flex justify-between">
                                <span>0%</span>
                                <span>50%</span>
                                <span>100%</span>
                            </div>
                        </div>
                    </div>
                </div>
                
                {reportData && (
                    <>
                        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                            <div className="flex justify-between items-center mb-4">
                                <div className="flex items-center">
                                    <h2 className="text-xl font-bold mr-6">Portfolio Overview</h2>
                                    <div className="flex items-center">
                                        <button
                                            onClick={() => this.handleDisplayFormatToggle()}
                                            className="flex items-center px-3 py-1 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500 text-sm bg-white hover:bg-gray-50"
                                        >
                                            <span className="mr-2">
                                                {displayFormat === 'percent' ? 'Showing Percentages' : 'Showing Dollar Values'}
                                            </span>
                                            <span className="text-xs text-gray-500">
                                                (Click to toggle)
                                            </span>
                                        </button>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <div className="text-sm text-gray-500">Report Date</div>
                                    <div className="text-lg font-semibold">{reportData.report_date}</div>
                                </div>
                            </div>
                            
                            <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-4">
                                <div className="bg-gray-50 p-4 rounded-lg">
                                    <div className="text-sm text-gray-500 mb-1">Total Value</div>
                                    <div className="text-2xl font-bold text-green-800">
                                        ${reportData.total_adjusted_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                                    </div>
                                </div>
                                
                                {/* Use the formatPerformance helper method to handle both array and object formats */}
                                {this.formatPerformance().map((perf) => (
                                    <div key={perf.name} className="bg-gray-50 p-4 rounded-lg">
                                        <div className="text-sm text-gray-500 mb-1">{perf.name} Performance</div>
                                        <div className={`text-2xl font-bold ${perf.percentage >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                            {perf.percentage >= 0 ? '+' : ''}{typeof perf.percentage === 'number' ? perf.percentage.toFixed(2) : perf.percentage}%
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
                            <div>
                                <h3 className="text-lg font-semibold mb-3">Asset Allocation</h3>
                                {allocationsChart ? (
                                    <div id="allocation-chart-container">
                                        {/* Using the window.DoughnutChart directly to avoid missing reference */}
                                        {typeof window.DoughnutChart === 'function' ? (
                                            <window.DoughnutChart 
                                                data={allocationsChart} 
                                                options={{
                                                    plugins: {
                                                        title: {
                                                            display: false
                                                        },
                                                        legend: {
                                                            position: 'right'
                                                        }
                                                    },
                                                    cutout: '60%'
                                                }}
                                                height="250px"
                                            />
                                        ) : (
                                            <div className="p-4 bg-yellow-100 text-yellow-800 rounded">
                                                Chart component not available. Please refresh the page.
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center" style={{ height: '250px' }}>
                                        <div className="animate-spin rounded-full h-10 w-10 border-t-2 border-b-2 border-green-800"></div>
                                    </div>
                                )}
                            </div>
                            
                            <div>
                                <h3 className="text-lg font-semibold mb-3">Liquidity Profile</h3>
                                {liquidityChart ? (
                                    <div id="liquidity-chart-container">
                                        {/* Using the window.PieChart directly to avoid missing reference */}
                                        {typeof window.PieChart === 'function' ? (
                                            <window.PieChart 
                                                data={liquidityChart} 
                                                options={{
                                                    plugins: {
                                                        title: {
                                                            display: false
                                                        },
                                                        legend: {
                                                            position: 'right'
                                                        }
                                                    }
                                                }}
                                                height="250px"
                                            />
                                        ) : (
                                            <div className="p-4 bg-yellow-100 text-yellow-800 rounded">
                                                Chart component not available. Please refresh the page.
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center" style={{ height: '250px' }}>
                                        <div className="animate-spin rounded-full h-10 w-10 border-t-2 border-b-2 border-green-800"></div>
                                    </div>
                                )}
                            </div>
                        </div>
                        
                        <div className="mb-6">
                            <h3 className="text-lg font-semibold mb-3">Performance History</h3>
                            {performanceChart ? (
                                <LineChart 
                                    data={performanceChart} 
                                    options={{
                                        scales: {
                                            y: {
                                                type: 'linear',
                                                display: true,
                                                position: 'left',
                                                title: {
                                                    display: true,
                                                    text: 'Total Value ($)'
                                                }
                                            },
                                            y1: {
                                                type: 'linear',
                                                display: true,
                                                position: 'right',
                                                grid: {
                                                    drawOnChartArea: false
                                                },
                                                title: {
                                                    display: true,
                                                    text: 'Percentage Change (%)'
                                                }
                                            }
                                        }
                                    }}
                                    height="300px"
                                />
                            ) : (
                                <div className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center" style={{ height: '300px' }}>
                                    <div className="animate-spin rounded-full h-10 w-10 border-t-2 border-b-2 border-green-800"></div>
                                </div>
                            )}
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <ReportTable
                                title="Asset Allocation"
                                data={this.formatAssetAllocation()}
                                columns={[
                                    { id: 'name', label: 'Asset Class', accessor: 'name' },
                                    { 
                                        id: 'percentage', 
                                        label: displayFormat === 'percent' ? 'Allocation (%)' : 'Allocation ($)', 
                                        accessor: 'percentage', 
                                        format: (value) => {
                                            if (displayFormat === 'percent') {
                                                return `${Number(value).toFixed(2)}%`;
                                            } else {
                                                // Calculate dollar value from percentage of total
                                                const dollarValue = (Number(value) / 100) * reportData.total_adjusted_value;
                                                return `$${dollarValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                                            }
                                        }
                                    }
                                ]}
                            />
                            
                            <ReportTable
                                title="Risk Metrics"
                                data={this.formatRiskMetrics()}
                                columns={[
                                    { id: 'name', label: 'Metric', accessor: 'name' },
                                    { id: 'value', label: 'Value', accessor: 'value', format: (value) => Number(value).toFixed(4) }
                                ]}
                            />
                        </div>
                    </>
                )}
            </div>
        );
    }
}