// Model Portfolio Management Page
// This page allows viewing, creating, and editing model portfolios

window.ModelPortfolioPage = () => {
    const [modelPortfolios, setModelPortfolios] = React.useState([]);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(null);
    const [selectedModel, setSelectedModel] = React.useState(null);
    const [showModelDetails, setShowModelDetails] = React.useState(false);
    const [compareMode, setCompareMode] = React.useState(false);
    const [portfolioOptions, setPortfolioOptions] = React.useState([]);
    const [selectedPortfolio, setSelectedPortfolio] = React.useState('');
    const [comparisonResult, setComparisonResult] = React.useState(null);
    const [comparisonLoading, setComparisonLoading] = React.useState(false);
    
    // Fetch model portfolios on component mount
    React.useEffect(() => {
        fetchModelPortfolios();
        fetchPortfolioOptions();
    }, []);
    
    // Fetch all model portfolios
    const fetchModelPortfolios = async () => {
        try {
            setLoading(true);
            setError(null);
            
            const response = await axios.get('/api/model-portfolios');
            
            if (response.data.success) {
                setModelPortfolios(response.data.portfolios || []);
            } else {
                setError('Failed to load model portfolios');
            }
        } catch (err) {
            console.error('Error fetching model portfolios:', err);
            setError('Failed to load model portfolios');
        } finally {
            setLoading(false);
        }
    };
    
    // Fetch portfolio options for comparison
    const fetchPortfolioOptions = async () => {
        try {
            const response = await axios.get('/api/entity-options?type=portfolio');
            
            if (response.data && response.data.success === true && response.data.options) {
                setPortfolioOptions(response.data.options);
            }
        } catch (err) {
            console.error('Error fetching portfolio options:', err);
        }
    };
    
    // View model portfolio details
    const viewModelDetails = async (modelId) => {
        try {
            setLoading(true);
            setError(null);
            
            const response = await axios.get(`/api/model-portfolios/${modelId}`);
            
            if (response.data.success) {
                setSelectedModel(response.data.portfolio);
                setShowModelDetails(true);
                setCompareMode(false);
            } else {
                setError('Failed to load model portfolio details');
            }
        } catch (err) {
            console.error('Error fetching model portfolio details:', err);
            setError('Failed to load model portfolio details');
        } finally {
            setLoading(false);
        }
    };
    
    // Compare portfolio with model
    const compareWithPortfolio = async () => {
        if (!selectedModel || !selectedPortfolio) {
            setError('Please select both a model portfolio and a portfolio to compare');
            return;
        }
        
        try {
            setComparisonLoading(true);
            setError(null);
            
            const response = await axios.get('/api/compare-portfolio', {
                params: {
                    portfolio_id: selectedPortfolio,
                    model_id: selectedModel.id,
                    date: '2025-05-01' // Use a date that we know has data
                }
            });
            
            if (response.data.success) {
                setComparisonResult(response.data.comparison);
                setCompareMode(true);
            } else {
                setError('Failed to compare portfolio with model');
            }
        } catch (err) {
            console.error('Error comparing portfolio with model:', err);
            setError('Failed to compare portfolio with model');
        } finally {
            setComparisonLoading(false);
        }
    };
    
    // Reset view
    const resetView = () => {
        setSelectedModel(null);
        setShowModelDetails(false);
        setCompareMode(false);
        setComparisonResult(null);
    };
    
    // Format percentage values
    const formatPercentage = (value) => {
        return `${value.toFixed(2)}%`;
    };
    
    // Calculate the color class for difference values
    const getDifferenceColorClass = (value) => {
        if (Math.abs(value) < 1) return 'text-gray-700'; // Minimal difference
        return value > 0 ? 'text-green-600' : 'text-red-600';
    };
    
    // Render the model portfolio list
    const renderModelPortfolioList = () => {
        if (loading) {
            return (
                <div className="flex items-center justify-center py-12">
                    <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-green-800"></div>
                </div>
            );
        }
        
        if (error) {
            return (
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
            );
        }
        
        if (modelPortfolios.length === 0) {
            return (
                <div className="bg-yellow-50 border-l-4 border-yellow-500 p-4 mb-6">
                    <div className="flex">
                        <div className="flex-shrink-0">
                            <i className="fas fa-exclamation-circle text-yellow-500"></i>
                        </div>
                        <div className="ml-3">
                            <p className="text-sm text-yellow-700">No model portfolios found. Create your first model portfolio.</p>
                        </div>
                    </div>
                </div>
            );
        }
        
        return (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {modelPortfolios.map(model => (
                    <div 
                        key={model.id} 
                        className="bg-white rounded-lg shadow-md p-6 hover:shadow-lg transition-shadow cursor-pointer"
                        onClick={() => viewModelDetails(model.id)}
                    >
                        <h3 className="text-xl font-semibold text-green-800 mb-2">{model.name}</h3>
                        <p className="text-gray-600 mb-4 line-clamp-2">{model.description || 'No description provided'}</p>
                        <div className="flex justify-between text-sm">
                            <span className="text-gray-500">Created: {new Date(model.creation_date).toLocaleDateString()}</span>
                            <span className="text-green-700">
                                <i className="fas fa-arrow-right"></i>
                            </span>
                        </div>
                    </div>
                ))}
            </div>
        );
    };
    
    // Render model portfolio details
    const renderModelDetails = () => {
        if (!selectedModel) return null;
        
        return (
            <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                <div className="flex justify-between items-center mb-6">
                    <div>
                        <h2 className="text-2xl font-bold text-green-800">{selectedModel.name}</h2>
                        <p className="text-gray-600">{selectedModel.description || 'No description provided'}</p>
                    </div>
                    <div className="flex space-x-2">
                        <button
                            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none"
                            onClick={() => setCompareMode(!compareMode)}
                        >
                            {compareMode ? 'View Details' : 'Compare with Portfolio'}
                        </button>
                        <button
                            className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300 focus:outline-none"
                            onClick={resetView}
                        >
                            Back to List
                        </button>
                    </div>
                </div>
                
                {compareMode ? renderComparisonForm() : renderPortfolioAllocation()}
            </div>
        );
    };
    
    // Render comparison form
    const renderComparisonForm = () => {
        return (
            <div>
                <div className="mb-6 p-4 bg-blue-50 rounded-lg">
                    <h3 className="text-lg font-semibold text-blue-800 mb-2">Compare with Portfolio</h3>
                    <div className="flex flex-col md:flex-row space-y-4 md:space-y-0 md:space-x-4">
                        <div className="flex-grow">
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                                Select Portfolio to Compare
                            </label>
                            <select
                                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                                value={selectedPortfolio}
                                onChange={(e) => setSelectedPortfolio(e.target.value)}
                            >
                                <option value="">Select Portfolio...</option>
                                {portfolioOptions.map(option => (
                                    <option key={option.key} value={option.key}>
                                        {option.display}
                                    </option>
                                ))}
                            </select>
                        </div>
                        <div className="flex items-end">
                            <button
                                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none w-full md:w-auto"
                                onClick={compareWithPortfolio}
                                disabled={!selectedPortfolio || comparisonLoading}
                            >
                                {comparisonLoading ? (
                                    <span className="flex items-center justify-center">
                                        <i className="fas fa-circle-notch fa-spin mr-2"></i>
                                        Loading...
                                    </span>
                                ) : 'Compare'}
                            </button>
                        </div>
                    </div>
                </div>
                
                {renderComparisonResults()}
            </div>
        );
    };
    
    // Render comparison results
    const renderComparisonResults = () => {
        if (!comparisonResult) return null;
        
        return (
            <div>
                <h3 className="text-xl font-semibold mb-4">Comparison Results</h3>
                <div className="mb-6">
                    <p className="text-gray-700">
                        Comparing <span className="font-semibold">{comparisonResult.portfolio_name}</span> with Model Portfolio <span className="font-semibold">{comparisonResult.model_name}</span>
                    </p>
                    <p className="text-gray-500 text-sm">Date: {comparisonResult.date}</p>
                </div>
                
                <div className="overflow-x-auto">
                    <table className="min-w-full bg-white">
                        <thead>
                            <tr className="bg-gray-100 text-gray-700">
                                <th className="py-3 px-4 text-left">Category</th>
                                <th className="py-3 px-4 text-right">Actual Portfolio</th>
                                <th className="py-3 px-4 text-right">Model Portfolio</th>
                                <th className="py-3 px-4 text-right">Difference</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-200">
                            {Object.entries(comparisonResult.categories).map(([category, data]) => (
                                <React.Fragment key={category}>
                                    <tr className="hover:bg-gray-50">
                                        <td className="py-3 px-4 font-medium">{category}</td>
                                        <td className="py-3 px-4 text-right">{formatPercentage(data.actual)}</td>
                                        <td className="py-3 px-4 text-right">{formatPercentage(data.model)}</td>
                                        <td className={`py-3 px-4 text-right ${getDifferenceColorClass(data.difference)}`}>
                                            {data.difference > 0 ? '+' : ''}{formatPercentage(data.difference)}
                                        </td>
                                    </tr>
                                    
                                    {/* Subcategories */}
                                    {Object.entries(data.subcategories).map(([subcat, subData]) => (
                                        <tr key={`${category}-${subcat}`} className="text-sm text-gray-600 hover:bg-gray-50">
                                            <td className="py-2 px-4 pl-8">{subcat}</td>
                                            <td className="py-2 px-4 text-right">{formatPercentage(subData.actual)}</td>
                                            <td className="py-2 px-4 text-right">{formatPercentage(subData.model)}</td>
                                            <td className={`py-2 px-4 text-right ${getDifferenceColorClass(subData.difference)}`}>
                                                {subData.difference > 0 ? '+' : ''}{formatPercentage(subData.difference)}
                                            </td>
                                        </tr>
                                    ))}
                                </React.Fragment>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    };
    
    // Render portfolio allocation details
    const renderPortfolioAllocation = () => {
        if (!selectedModel) return null;
        
        return (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Asset Allocation */}
                <div className="bg-gray-50 p-4 rounded-lg">
                    <h3 className="text-lg font-semibold mb-3">Asset Allocation</h3>
                    <div className="space-y-3">
                        {Object.entries(selectedModel.allocations).map(([category, data]) => (
                            <div key={category} className="border-b pb-2">
                                <div className="flex justify-between">
                                    <span className="font-medium">{category.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase())}</span>
                                    <span>{formatPercentage(data.total_pct)}</span>
                                </div>
                                {Object.entries(data.subcategories).map(([subcat, value]) => (
                                    <div key={`${category}-${subcat}`} className="flex justify-between ml-4 text-sm text-gray-600 mt-1">
                                        <span>{subcat.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase())}</span>
                                        <span>{formatPercentage(value)}</span>
                                    </div>
                                ))}
                            </div>
                        ))}
                    </div>
                </div>
                
                {/* Liquidity and Performance */}
                <div className="space-y-6">
                    {/* Liquidity */}
                    <div className="bg-gray-50 p-4 rounded-lg">
                        <h3 className="text-lg font-semibold mb-3">Liquidity</h3>
                        <div className="space-y-2">
                            <div className="flex justify-between">
                                <span>Liquid Assets</span>
                                <span>{formatPercentage(selectedModel.liquidity.liquid_assets)}</span>
                            </div>
                            <div className="flex justify-between">
                                <span>Illiquid Assets</span>
                                <span>{formatPercentage(selectedModel.liquidity.illiquid_assets)}</span>
                            </div>
                        </div>
                    </div>
                    
                    {/* Performance */}
                    <div className="bg-gray-50 p-4 rounded-lg">
                        <h3 className="text-lg font-semibold mb-3">Performance</h3>
                        <div className="space-y-2">
                            {Object.entries(selectedModel.performance || {}).map(([period, value]) => (
                                <div key={period} className="flex justify-between">
                                    <span>{period}</span>
                                    <span className={value >= 0 ? 'text-green-600' : 'text-red-600'}>
                                        {value >= 0 ? '+' : ''}{formatPercentage(value)}
                                    </span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        );
    };
    
    return (
        <div className="container mx-auto px-4 py-8">
            {/* Page header */}
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h1 className="text-2xl font-bold text-gray-800">Model Portfolios</h1>
                    <p className="text-gray-600">View and compare model portfolios</p>
                </div>
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
            
            {/* Content */}
            {showModelDetails ? renderModelDetails() : renderModelPortfolioList()}
        </div>
    );
};

// No export needed with window assignment