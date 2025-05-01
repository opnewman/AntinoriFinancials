const Dashboard = () => {
    const [loading, setLoading] = React.useState(true);
    const [reportDate, setReportDate] = React.useState(new Date().toISOString().split('T')[0]);
    const [reportLevel, setReportLevel] = React.useState('client');
    const [levelKey, setLevelKey] = React.useState('');
    const [levelOptions, setLevelOptions] = React.useState([]);
    const [ownershipTree, setOwnershipTree] = React.useState([]);
    const [reportData, setReportData] = React.useState(null);
    const [allocationsChart, setAllocationsChart] = React.useState(null);
    const [liquidityChart, setLiquidityChart] = React.useState(null);
    const [performanceChart, setPerformanceChart] = React.useState(null);
    const [error, setError] = React.useState('');
    
    // Fetch ownership tree on component mount
    React.useEffect(() => {
        const fetchOwnershipTree = async () => {
            try {
                const data = await api.getOwnershipTree();
                setOwnershipTree(data);
                
                // Set default level options based on the first client
                if (data.length > 0) {
                    updateLevelOptions('client', data);
                    setLevelKey(data[0].name);
                }
            } catch (err) {
                console.error('Error fetching ownership tree:', err);
                setError('Failed to load ownership data. Please upload ownership data first.');
            } finally {
                setLoading(false);
            }
        };
        
        fetchOwnershipTree();
    }, []);
    
    // Update level options when the level changes
    const updateLevelOptions = (level, tree = ownershipTree) => {
        if (!tree || tree.length === 0) return;
        
        let options = [];
        
        if (level === 'client') {
            options = tree.map(client => ({
                value: client.name,
                label: client.name
            }));
        } else if (level === 'portfolio' || level === 'group') {
            // Flatten all portfolios and groups from all clients
            tree.forEach(client => {
                client.children.forEach(child => {
                    if (child.type === level) {
                        options.push({
                            value: child.name,
                            label: `${child.name} (${client.name})`
                        });
                    }
                });
            });
        } else if (level === 'account') {
            // Flatten all accounts from all clients
            tree.forEach(client => {
                client.children.forEach(child => {
                    child.children.forEach(account => {
                        options.push({
                            value: account.account_number,
                            label: `${account.name} (${account.account_number})`
                        });
                    });
                });
            });
        }
        
        setLevelOptions(options);
        
        // Set default value to first option if available
        if (options.length > 0) {
            setLevelKey(options[0].value);
        } else {
            setLevelKey('');
        }
    };
    
    // Handle level change
    const handleLevelChange = (e) => {
        const newLevel = e.target.value;
        setReportLevel(newLevel);
        updateLevelOptions(newLevel);
    };
    
    // Generate report
    const generateReport = async () => {
        if (!levelKey) {
            setError('Please select a valid option');
            return;
        }
        
        setLoading(true);
        setError('');
        setReportData(null);
        setAllocationsChart(null);
        setLiquidityChart(null);
        setPerformanceChart(null);
        
        try {
            // Get portfolio report
            const report = await api.getPortfolioReport(reportDate, reportLevel, levelKey);
            setReportData(report);
            
            // Get allocation chart data
            const allocations = await api.getAllocationChartData(reportDate, reportLevel, levelKey);
            setAllocationsChart({
                labels: allocations.labels,
                datasets: [{
                    data: allocations.datasets[0].data,
                    backgroundColor: allocations.datasets[0].backgroundColor,
                    borderWidth: 1,
                    borderColor: '#fff'
                }]
            });
            
            // Get liquidity chart data
            const liquidity = await api.getLiquidityChartData(reportDate, reportLevel, levelKey);
            setLiquidityChart({
                labels: liquidity.labels,
                datasets: [{
                    data: liquidity.datasets[0].data,
                    backgroundColor: liquidity.datasets[0].backgroundColor,
                    borderWidth: 1,
                    borderColor: '#fff'
                }]
            });
            
            // Get performance chart data
            const performance = await api.getPerformanceChartData(reportDate, reportLevel, levelKey, 'YTD');
            setPerformanceChart({
                labels: performance.labels,
                datasets: performance.datasets
            });
            
        } catch (err) {
            console.error('Error generating report:', err);
            setError(err.response?.data?.detail || 'Failed to generate report. Please check your inputs.');
        } finally {
            setLoading(false);
        }
    };
    
    // Format data for tables
    const formatAssetAllocation = () => {
        if (!reportData || !reportData.asset_allocation) return [];
        
        return Object.entries(reportData.asset_allocation).map(([asset, percentage]) => ({
            name: asset,
            percentage
        }));
    };
    
    const formatLiquidity = () => {
        if (!reportData || !reportData.liquidity) return [];
        
        return Object.entries(reportData.liquidity).map(([type, percentage]) => ({
            name: type,
            percentage
        }));
    };
    
    const formatPerformance = () => {
        if (!reportData || !reportData.performance) return [];
        
        return reportData.performance.map(perf => ({
            name: perf.period,
            value: perf.value,
            percentage: perf.percentage
        }));
    };
    
    const formatRiskMetrics = () => {
        if (!reportData || !reportData.risk_metrics) return [];
        
        return reportData.risk_metrics.map(risk => ({
            name: risk.metric,
            value: risk.value
        }));
    };
    
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
                
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
                    <div>
                        <label htmlFor="reportDate" className="block text-sm font-medium text-gray-700 mb-1">
                            Report Date
                        </label>
                        <input
                            type="date"
                            id="reportDate"
                            value={reportDate}
                            onChange={(e) => setReportDate(e.target.value)}
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
                            onChange={handleLevelChange}
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
                            onChange={(e) => setLevelKey(e.target.value)}
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
                    
                    <div className="flex items-end">
                        <button
                            onClick={generateReport}
                            disabled={loading || !levelKey}
                            className={`w-full py-2 px-4 rounded-md font-medium ${
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
            </div>
            
            {reportData && (
                <>
                    <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                        <div className="flex justify-between items-center mb-4">
                            <h2 className="text-xl font-bold">Portfolio Overview</h2>
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
                            
                            {reportData.performance.map((perf) => (
                                <div key={perf.period} className="bg-gray-50 p-4 rounded-lg">
                                    <div className="text-sm text-gray-500 mb-1">{perf.period} Performance</div>
                                    <div className={`text-2xl font-bold ${perf.percentage >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                        {perf.percentage >= 0 ? '+' : ''}{perf.percentage.toFixed(2)}%
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
                            data={formatAssetAllocation()}
                            columns={[
                                { id: 'name', label: 'Asset Class', accessor: 'name' },
                                { id: 'percentage', label: 'Allocation (%)', accessor: 'percentage', format: (value) => `${Number(value).toFixed(2)}%` }
                            ]}
                        />
                        
                        <ReportTable
                            title="Risk Metrics"
                            data={formatRiskMetrics()}
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
};
