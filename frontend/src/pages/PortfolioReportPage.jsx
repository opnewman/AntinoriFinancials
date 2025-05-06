// Professional Portfolio Report Page
// No import needed here since we're using script tags in index.html
// and all components are declared globally in the browser environment

window.PortfolioReportPage = () => {
    const [loading, setLoading] = React.useState(false);
    const [reportData, setReportData] = React.useState(null);
    const [error, setError] = React.useState(null);
    const [selectedLevel, setSelectedLevel] = React.useState('portfolio');
    const [selectedLevelKey, setSelectedLevelKey] = React.useState('');
    const [levelOptions, setLevelOptions] = React.useState([]);
    const [reportDate, setReportDate] = React.useState('2025-05-01');
    
    // Fetch entity options when level changes
    React.useEffect(() => {
        const fetchOptions = async () => {
            try {
                const response = await axios.get(`/api/entity-options?type=${selectedLevel}`);
                console.log('Entity options response:', response);
                
                if (response.data && response.data.success === true && response.data.options) {
                    console.log('Successfully loaded options:', response.data.options);
                    setLevelOptions(response.data.options);
                    
                    // Set default selection if options are available and nothing is selected
                    if (response.data.options.length > 0 && !selectedLevelKey) {
                        setSelectedLevelKey(response.data.options[0].key);
                    }
                } else {
                    console.error('Invalid response format for entity options:', response.data);
                    setError('Failed to load entity options');
                }
            } catch (err) {
                console.error('Entity options error:', err);
                setError('Failed to load entity options');
            }
        };
        
        fetchOptions();
    }, [selectedLevel]);
    
    // Generate report when level, level key or date changes
    const generateReport = async () => {
        if (!selectedLevelKey) return;
        
        setLoading(true);
        setError(null);
        
        try {
            const response = await axios.get('/api/portfolio-report', {
                params: {
                    level: selectedLevel,
                    level_key: selectedLevelKey,
                    date: reportDate
                }
            });
            
            console.log('Portfolio report response:', response);
            
            if (response.data) {
                setReportData(response.data);
            } else {
                setError('No data received from server');
            }
        } catch (err) {
            console.error('Error generating report:', err);
            setError('Failed to generate portfolio report');
        } finally {
            setLoading(false);
        }
    };
    
    // Handle level change
    const handleLevelChange = (e) => {
        setSelectedLevel(e.target.value);
        setSelectedLevelKey(''); // Reset level key when level changes
    };
    
    // Handle level key change
    const handleLevelKeyChange = (e) => {
        setSelectedLevelKey(e.target.value);
    };
    
    // Handle date change
    const handleDateChange = (e) => {
        setReportDate(e.target.value);
    };
    
    // Handle form submission
    const handleSubmit = (e) => {
        e.preventDefault();
        generateReport();
    };
    
    // Export report as Excel
    const exportToExcel = () => {
        alert('Excel export functionality would be implemented here');
        // In a real implementation, this would call a backend endpoint that generates an Excel file
    };
    
    // Format date for display
    const formatDate = (dateStr) => {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', {
            month: 'long',
            day: 'numeric',
            year: 'numeric'
        });
    };
    
    return (
        <div className="container mx-auto px-4 py-8">
            {/* Page header */}
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h1 className="text-2xl font-bold text-gray-800">Portfolio Report</h1>
                    <p className="text-gray-600">Generate and view detailed portfolio reports</p>
                </div>
                
                <button
                    className="px-4 py-2 bg-green-800 text-white rounded hover:bg-green-700 focus:outline-none flex items-center"
                    onClick={exportToExcel}
                >
                    <i className="fas fa-file-excel mr-2"></i>
                    Export to Excel
                </button>
            </div>
            
            {/* Report options */}
            <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                <form onSubmit={handleSubmit} className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Level
                        </label>
                        <select
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={selectedLevel}
                            onChange={handleLevelChange}
                        >
                            <option value="client">Client</option>
                            <option value="portfolio">Portfolio</option>
                            <option value="account">Account</option>
                        </select>
                    </div>
                    
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            {selectedLevel === 'client' ? 'Client' : 
                             selectedLevel === 'portfolio' ? 'Portfolio' : 'Account'}
                        </label>
                        <select
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={selectedLevelKey}
                            onChange={handleLevelKeyChange}
                        >
                            <option value="">Select...</option>
                            {levelOptions.map(option => (
                                <option key={option.key} value={option.key}>
                                    {option.display}
                                </option>
                            ))}
                        </select>
                    </div>
                    
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Report Date
                        </label>
                        <input
                            type="date"
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={reportDate}
                            onChange={handleDateChange}
                        />
                    </div>
                    
                    <div className="flex items-end">
                        <button
                            type="submit"
                            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none w-full"
                            disabled={!selectedLevelKey || loading}
                        >
                            {loading ? (
                                <span className="flex items-center justify-center">
                                    <i className="fas fa-circle-notch fa-spin mr-2"></i>
                                    Loading...
                                </span>
                            ) : 'Generate Report'}
                        </button>
                    </div>
                </form>
            </div>
            
            {/* Error display */}
            {error && (
                <div className="bg-red-50 border-l-4 border-red-500 p-4 mb-6">
                    <div className="flex">
                        <div className="flex-shrink-0">
                            <i className="fas fa-exclamation-circle text-red-500"></i>
                        </div>
                        <div className="ml-3">
                            <p className="text-sm text-red-700">{error}</p>
                        </div>
                    </div>
                </div>
            )}
            
            {/* Report display */}
            {reportData && (
                <PortfolioReport reportData={reportData} loading={loading} />
            )}
        </div>
    );
};

// No export needed with window assignment