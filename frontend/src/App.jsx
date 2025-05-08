const { BrowserRouter, Switch, Route, Redirect } = ReactRouterDOM;

// No imports needed as components are loaded via script tags in index.html
// and globally available in the browser environment

const App = () => {
    const [loading, setLoading] = React.useState(true);
    const [isApiAvailable, setIsApiAvailable] = React.useState(true);
    
    // Check API health on load
    React.useEffect(() => {
        const checkApiHealth = async () => {
            try {
                setLoading(true);
                const response = await window.api.checkHealth();
                setIsApiAvailable(response.status === "healthy");
            } catch (error) {
                console.error("API Health check failed:", error);
                setIsApiAvailable(false);
            } finally {
                setLoading(false);
            }
        };
        
        checkApiHealth();
    }, []);
    
    if (loading) {
        return (
            <div className="flex items-center justify-center min-h-screen">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                    <p className="mt-4 text-lg font-medium text-gray-700">Loading nori...</p>
                </div>
            </div>
        );
    }
    
    if (!isApiAvailable) {
        return (
            <div className="flex items-center justify-center min-h-screen">
                <div className="text-center p-8 max-w-md">
                    <div className="text-red-600 text-5xl mb-4">
                        <i className="fas fa-exclamation-triangle"></i>
                    </div>
                    <h1 className="text-2xl font-bold text-gray-800 mb-2">API Connection Error</h1>
                    <p className="text-gray-600 mb-4">
                        Unable to connect to the ANTINORI backend API. Please check that the backend server is running.
                    </p>
                    <div className="bg-gray-100 p-4 rounded-lg text-left text-sm font-mono">
                        <p className="text-gray-700">1. Ensure the FastAPI backend is running</p>
                        <p className="text-gray-700">2. Check network connectivity</p>
                        <p className="text-gray-700">3. Verify API URL configuration</p>
                    </div>
                    <button 
                        className="mt-6 px-4 py-2 bg-green-700 text-white rounded hover:bg-green-800"
                        onClick={() => window.location.reload()}
                    >
                        Retry Connection
                    </button>
                </div>
            </div>
        );
    }
    
    return (
        <BrowserRouter>
            <div className="flex flex-col min-h-screen">
                <NavigationBar />
                
                <main className="flex-grow p-4 md:p-6 bg-gray-50">
                    <Switch>
                        <Route exact path="/">
                            <Dashboard />
                        </Route>
                        <Route path="/upload">
                            <div className="container mx-auto p-4">
                                <h1 className="text-2xl font-bold mb-6">File Upload</h1>
                                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                                    <FileUploader
                                        title="Data Dump"
                                        description="Upload financial positions data (data_dump.xlsx)"
                                        endpoint="/api/upload/data-dump"
                                        icon="fa-file-excel"
                                    />
                                    <FileUploader
                                        title="Ownership Tree"
                                        description="Upload ownership hierarchy data (ownership.xlsx)"
                                        endpoint="/api/upload/ownership"
                                        icon="fa-sitemap"
                                    />
                                    <FileUploader
                                        title="Risk Statistics"
                                        description="Upload risk statistics data (risk_stats.xlsx)"
                                        endpoint="/api/upload/risk-stats"
                                        icon="fa-chart-line"
                                    />
                                </div>
                            </div>
                        </Route>
                        <Route path="/ownership">
                            <SimpleOwnershipTreePage />
                        </Route>
                        <Route path="/reports">
                            <PortfolioReportPage />
                        </Route>
                        <Route path="/model-portfolios">
                            <ModelPortfolioPage />
                        </Route>
                        <Redirect to="/" />
                    </Switch>
                </main>
                
                <footer className="bg-gray-800 text-white py-4 text-center text-sm">
                    <div className="container mx-auto">
                        <p>ANTINORI Financial Portfolio Reporting System &copy; {new Date().getFullYear()}</p>
                        <p className="text-gray-400 text-xs mt-1">
                            Powered by Flask, React, and PostgreSQL
                        </p>
                    </div>
                </footer>
            </div>
        </BrowserRouter>
    );
};
