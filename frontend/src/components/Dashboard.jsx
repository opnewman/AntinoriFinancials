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
        this.setState({ reportLevel: newLevel });
        this.updateLevelOptions(newLevel);
    };
    
    // Generate report
    generateReport = async () => {
        const { reportDate, reportLevel, levelKey, displayFormat } = this.state;
        
        if (!levelKey) {
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
                const report = await portfolioReportPromise;
                if (this._isMounted) {
                    this.setState({ reportData: report });
                }
            } catch (err) {
                console.error('Portfolio report error:', err);
                if (this._isMounted) {
                    this.setState({ error: err.message || 'Failed to load portfolio report' });
                }
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
        if (!reportData || !reportData.performance) return [];
        
        // Handle case where performance is an object rather than an array
        if (!Array.isArray(reportData.performance)) {
            // Convert the object to an array of objects
            return Object.entries(reportData.performance).map(([period, value]) => ({
                name: period,
                value: value,
                percentage: value // Use the same value for percentage since that's what we have
            }));
        }
        
        // If it's already an array, use the original format
        return reportData.performance.map(perf => ({
            name: perf.period,
            value: perf.value,
            percentage: perf.percentage
        }));
    };
    
    formatRiskMetrics = () => {
        const { reportData } = this.state;
        if (!reportData || !reportData.risk_metrics) return [];
        
        return reportData.risk_metrics.map(risk => ({
            name: risk.metric,
            value: risk.value
        }));
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
                    <h2 className="text-xl font-bold mb-4">Portfolio Report Generator</h2>
                    
                    {error && (
                        <div className="bg-red-50 text-red-600 p-3 rounded-md mb-4">
                            <i className="fas fa-exclamation-circle mr-2"></i>
                            {error}
                        </div>
                    )}
                    
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                        <div>
                            <label htmlFor="reportDate" className="block text-sm font-medium text-gray-700 mb-1">
                                Report Date
                            </label>
                            <input
                                type="date"
                                id="reportDate"
                                value={reportDate}
                                onChange={(e) => this.setState({ reportDate: e.target.value })}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            />
                        </div>
                        
                        <div>
                            <label htmlFor="reportLevel" className="block text-sm font-medium text-gray-700 mb-1">
                                Report Level
                            </label>
                            <select
                                id="reportLevel"
                                value={reportLevel}
                                onChange={this.handleLevelChange}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            >
                                <option value="client">Client</option>
                                <option value="portfolio">Portfolio</option>
                                <option value="group">Group</option>
                                <option value="account">Account</option>
                            </select>
                        </div>
                        
                        <div>
                            <label htmlFor="levelKey" className="block text-sm font-medium text-gray-700 mb-1">
                                Select {reportLevel.charAt(0).toUpperCase() + reportLevel.slice(1)}
                            </label>
                            <select
                                id="levelKey"
                                value={levelKey}
                                onChange={(e) => this.setState({ levelKey: e.target.value })}
                                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
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
                    
                    <div className="flex justify-end">
                        <button
                            onClick={this.generateReport}
                            disabled={loading || !levelKey}
                            className={`py-2 px-6 rounded-md font-medium ${
                                loading || !levelKey
                                    ? 'bg-gray-300 text-gray-600 cursor-not-allowed'
                                    : 'bg-green-700 text-white hover:bg-green-800'
                            }`}
                        >
                            {loading ? (
                                <span className="flex items-center justify-center">
                                    <i className="fas fa-spinner fa-spin mr-2"></i>
                                    Generating...
                                </span>
                            ) : (
                                <span className="flex items-center justify-center">
                                    <i className="fas fa-chart-line mr-2"></i>
                                    Generate Report
                                </span>
                            )}
                        </button>
                    </div>
                </div>
                
                {reportData && (
                    <>
                        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                            <div className="flex justify-between items-center mb-4">
                                <div className="flex items-center">
                                    <h2 className="text-xl font-bold mr-6">Portfolio Overview</h2>
                                    <div className="flex items-center">
                                        <label htmlFor="displayFormat" className="mr-2 text-sm font-medium text-gray-700">
                                            Display As:
                                        </label>
                                        <select
                                            id="displayFormat"
                                            value={displayFormat}
                                            onChange={this.handleDisplayFormatChange}
                                            className="px-3 py-1 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500 text-sm"
                                        >
                                            <option value="percent">Percentages</option>
                                            <option value="dollar">Dollar Values</option>
                                        </select>
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
                                    <DoughnutChart 
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
                                    <div className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center" style={{ height: '250px' }}>
                                        <div className="animate-spin rounded-full h-10 w-10 border-t-2 border-b-2 border-green-800"></div>
                                    </div>
                                )}
                            </div>
                            
                            <div>
                                <h3 className="text-lg font-semibold mb-3">Liquidity Profile</h3>
                                {liquidityChart ? (
                                    <PieChart 
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