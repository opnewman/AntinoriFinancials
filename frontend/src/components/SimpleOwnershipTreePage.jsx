// Simple Ownership Tree Page component
const SimpleOwnershipTreePage = () => {
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState('');
    const [treeData, setTreeData] = React.useState(null);
    
    // Fetch data on component mount
    React.useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                console.log('Fetching ownership tree data...');
                
                const response = await axios.get('/api/ownership-tree');
                console.log('Ownership tree API response:', response);
                
                if (response.data && response.data.success) {
                    setTreeData(response.data.data);
                } else if (response.data) {
                    setTreeData(response.data);
                } else {
                    throw new Error('No data received from API');
                }
            } catch (err) {
                console.error('Error fetching ownership tree:', err);
                setError('Failed to load ownership structure: ' + (err.message || 'Unknown error'));
            } finally {
                setLoading(false);
            }
        };
        
        fetchData();
    }, []);
    
    if (loading) {
        return (
            <div className="container mx-auto p-8">
                <h1 className="text-2xl font-bold mb-6">Ownership Tree</h1>
                <div className="flex justify-center items-center h-64">
                    <div className="text-center">
                        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                        <p className="mt-4 text-lg font-medium text-gray-700">Loading ownership structure...</p>
                    </div>
                </div>
            </div>
        );
    }
    
    if (error) {
        return (
            <div className="container mx-auto p-8">
                <h1 className="text-2xl font-bold mb-6">Ownership Tree</h1>
                <div className="bg-red-50 text-red-600 p-4 rounded-md">
                    <i className="fas fa-exclamation-circle mr-2"></i>
                    {error}
                </div>
            </div>
        );
    }
    
    return (
        <div className="container mx-auto p-8">
            <h1 className="text-2xl font-bold mb-6">Ownership Tree</h1>
            <div className="bg-white rounded-lg shadow-md p-6">
                <div className="mb-4 pb-4 border-b">
                    <ReactRouterDOM.Link to="/" className="text-green-700 hover:underline">
                        <i className="fas fa-arrow-left mr-2"></i>
                        Back to Dashboard
                    </ReactRouterDOM.Link>
                </div>
                
                {treeData ? (
                    <OwnershipTree data={treeData} />
                ) : (
                    <div className="text-center text-gray-500 py-8">
                        <i className="fas fa-sitemap text-4xl mb-4"></i>
                        <p>No ownership structure data available</p>
                        <p className="text-sm mt-2">Upload an ownership file to see the hierarchy</p>
                    </div>
                )}
            </div>
        </div>
    );
};