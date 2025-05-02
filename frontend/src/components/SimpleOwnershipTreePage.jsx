// Professional Ownership Tree Page component
const SimpleOwnershipTreePage = () => {
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState('');
    const [treeData, setTreeData] = React.useState(null);
    const [selectedClient, setSelectedClient] = React.useState(null);
    const [searchTerm, setSearchTerm] = React.useState('');
    const [clientList, setClientList] = React.useState([]);
    const [stats, setStats] = React.useState({
        totalValue: 0,
        accounts: 0,
        entities: 0
    });
    
    // Fetch data on component mount
    React.useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                console.log('Fetching ownership tree data...');
                
                const response = await axios.get('/api/ownership-tree');
                console.log('Ownership tree API response:', response);
                
                if (response.data && response.data.success) {
                    processTreeData(response.data.data);
                } else if (response.data) {
                    processTreeData(response.data);
                } else {
                    throw new Error('No data received from API');
                }
            } catch (err) {
                console.error('Error fetching ownership tree:', err);
                setError('Failed to load ownership structure: ' + (err.message || 'Unknown error'));
                setLoading(false);
            }
        };
        
        fetchData();
    }, []);
    
    // Process tree data to extract clients and statistics
    const processTreeData = (data) => {
        if (!data) {
            setLoading(false);
            return;
        }
        
        // Extract client list
        let clients = [];
        let totalValue = 0;
        let totalAccounts = 0;
        let totalEntities = 0;
        
        // Check if data is the root "All Clients" node or already an array of clients
        if (data.name === "All Clients" && data.children) {
            clients = data.children.map(client => ({
                id: client.name,
                name: client.name,
                value: client.value || 0
            }));
            
            // Count statistics from the tree
            const countStats = (node) => {
                if (!node) return;
                
                if (node.value) {
                    totalValue += node.value;
                }
                
                // Count this node as an entity
                totalEntities++;
                
                // If it's an account, count it
                if (node.type === 'account' || (!node.children && !node.groups && !node.portfolios)) {
                    totalAccounts++;
                }
                
                // Process children
                if (node.children) {
                    node.children.forEach(countStats);
                }
                
                // Process groups
                if (node.groups) {
                    node.groups.forEach(countStats);
                }
                
                // Process accounts
                if (node.accounts) {
                    node.accounts.forEach(countStats);
                    totalAccounts += node.accounts.length;
                }
                
                // Process portfolios
                if (node.portfolios) {
                    node.portfolios.forEach(countStats);
                }
            };
            
            countStats(data);
        } else if (Array.isArray(data)) {
            clients = data.map(client => ({
                id: client.name,
                name: client.name,
                value: client.value || 0
            }));
            
            // Count totals from array
            data.forEach(client => {
                if (client.value) totalValue += client.value;
                totalEntities++;
                // Further counting would need recursion similar to above
            });
        }
        
        setClientList(clients);
        setStats({
            totalValue,
            accounts: totalAccounts,
            entities: totalEntities
        });
        setTreeData(data);
        setLoading(false);
    };
    
    // Handle client selection
    const handleClientSelect = (clientId) => {
        setSelectedClient(clientId === selectedClient ? null : clientId);
    };
    
    // Filter clients by search term
    const getFilteredClients = () => {
        if (!searchTerm) return clientList;
        
        return clientList.filter(client => 
            client.name.toLowerCase().includes(searchTerm.toLowerCase())
        );
    };
    
    // Export visualization as PNG
    const exportAsPNG = () => {
        // In a real implementation, this would use html2canvas or similar library
        alert('Export as PNG functionality would be implemented here');
    };
    
    // Handlers for buttons
    const expandAll = () => {
        // In a real implementation, this would expand all tree nodes
        console.log("Expand all nodes");
    };
    
    const collapseAll = () => {
        // In a real implementation, this would collapse all tree nodes
        console.log("Collapse all nodes");
    };
    
    const refreshData = () => {
        setLoading(true);
        // This would re-fetch the data
        console.log("Refreshing data");
        setTimeout(() => setLoading(false), 500);
    };
    
    // Format currency value
    const formatCurrency = (value) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            maximumFractionDigits: 0,
            minimumFractionDigits: 0
        }).format(value).replace('$', '');
    };
    
    if (loading) {
        return (
            <div className="h-full flex items-center justify-center">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                    <p className="mt-4 text-lg font-medium text-gray-700">Loading ownership structure...</p>
                </div>
            </div>
        );
    }
    
    if (error) {
        return (
            <div className="h-full p-8">
                <h1 className="text-2xl font-bold mb-6">Ownership Tree</h1>
                <div className="bg-red-50 text-red-600 p-4 rounded-md">
                    <i className="fas fa-exclamation-circle mr-2"></i>
                    {error}
                </div>
            </div>
        );
    }
    
    const filteredClients = getFilteredClients();
    
    return (
        <div className="h-full flex" style={{ height: 'calc(100vh - 64px)' }}>
            {/* Sidebar */}
            <div className="w-64 bg-gray-50 border-r border-gray-200 flex flex-col">
                <div className="p-4 border-b border-gray-200">
                    <h1 className="text-xl font-bold text-green-800">Ownership Tree</h1>
                </div>
                
                <div className="p-4 border-b border-gray-200">
                    <ReactRouterDOM.Link 
                        to="/" 
                        className="inline-flex items-center px-4 py-2 border border-green-800 text-sm font-medium rounded-md text-green-800 bg-white hover:bg-green-50 focus:outline-none"
                    >
                        <i className="fas fa-arrow-left mr-2"></i>
                        Back to Dashboard
                    </ReactRouterDOM.Link>
                </div>
                
                <div className="p-4 border-b border-gray-200">
                    <div className="relative">
                        <input
                            type="text"
                            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-green-500 focus:border-green-500"
                            placeholder="Search clients..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                        <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                            <i className="fas fa-search text-gray-400"></i>
                        </div>
                    </div>
                </div>
                
                <div className="p-4 border-b border-gray-200 grid grid-cols-2 gap-2">
                    <button 
                        onClick={expandAll}
                        className="inline-flex items-center justify-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none"
                    >
                        Expand All
                    </button>
                    <button 
                        onClick={collapseAll}
                        className="inline-flex items-center justify-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none"
                    >
                        Collapse All
                    </button>
                    <button 
                        onClick={refreshData}
                        className="inline-flex items-center justify-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none col-span-2"
                    >
                        <i className="fas fa-sync-alt mr-2"></i>
                        Refresh Data
                    </button>
                </div>
                
                <div className="flex-grow overflow-y-auto">
                    <div className="p-4 border-b border-gray-200">
                        <h2 className="text-lg font-semibold">Clients</h2>
                    </div>
                    
                    <div className="divide-y divide-gray-200">
                        {filteredClients.map(client => (
                            <div 
                                key={client.id}
                                className={`p-3 cursor-pointer hover:bg-gray-100 ${selectedClient === client.id ? 'bg-green-50 border-l-4 border-green-800' : ''}`}
                                onClick={() => handleClientSelect(client.id)}
                            >
                                <div className="flex items-center">
                                    <i className="fas fa-user-tie text-green-800 mr-2"></i>
                                    <span className="truncate">{client.name}</span>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
            
            {/* Main content */}
            <div className="flex-1 flex flex-col overflow-hidden">
                {/* Top navigation and path */}
                <div className="bg-white p-4 border-b border-gray-200 flex justify-between items-center">
                    <div className="flex items-center space-x-2 text-sm text-gray-500">
                        <ReactRouterDOM.Link to="/" className="text-green-700 hover:underline">Home</ReactRouterDOM.Link>
                        <i className="fas fa-chevron-right text-gray-400"></i>
                        <span className="text-green-700">Ownership Tree</span>
                        {selectedClient && (
                            <>
                                <i className="fas fa-chevron-right text-gray-400"></i>
                                <span>{selectedClient}</span>
                            </>
                        )}
                    </div>
                    
                    <button 
                        onClick={exportAsPNG}
                        className="inline-flex items-center px-4 py-2 border border-green-800 text-sm font-medium rounded-md text-white bg-green-800 hover:bg-green-700 focus:outline-none"
                    >
                        <i className="fas fa-download mr-2"></i>
                        Export PNG
                    </button>
                </div>
                
                {/* Statistics cards */}
                <div className="bg-gray-50 p-4 grid grid-cols-3 gap-4">
                    <div className="bg-white p-4 rounded shadow">
                        <h3 className="text-sm font-medium text-gray-500 uppercase">TOTAL VALUE</h3>
                        <div className="text-2xl font-bold text-green-800">${formatCurrency(stats.totalValue)}</div>
                    </div>
                    <div className="bg-white p-4 rounded shadow">
                        <h3 className="text-sm font-medium text-gray-500 uppercase">ACCOUNTS</h3>
                        <div className="text-2xl font-bold text-gray-700">{stats.accounts.toLocaleString()}</div>
                    </div>
                    <div className="bg-white p-4 rounded shadow">
                        <h3 className="text-sm font-medium text-gray-500 uppercase">ENTITIES</h3>
                        <div className="text-2xl font-bold text-gray-700">{stats.entities.toLocaleString()}</div>
                    </div>
                </div>
                
                {/* Tree visualization */}
                <div className="flex-grow overflow-hidden relative">
                    {treeData ? (
                        <div className="absolute inset-0 bg-white rounded-lg shadow-md">
                            <OwnershipTree 
                                data={selectedClient ? 
                                    treeData.children?.find(c => c.name === selectedClient) || treeData : 
                                    treeData} 
                            />
                            
                            {/* Zoom controls */}
                            <div className="absolute bottom-4 left-4 bg-white rounded-md shadow-md z-10 flex">
                                <button 
                                    className="px-3 py-2 border-r border-gray-200 text-gray-600 hover:bg-gray-50" 
                                    title="Zoom In"
                                    onClick={() => {
                                        const container = document.querySelector('.ownership-tree-visualization');
                                        if (container) {
                                            // Get current scale
                                            const currentTransform = container.style.transform;
                                            const currentScale = currentTransform ? 
                                                parseFloat(currentTransform.replace(/[^0-9.]/g, '')) || 1 : 1;
                                            
                                            // Apply new scale
                                            container.style.transform = `scale(${currentScale * 1.2})`;
                                            container.style.transformOrigin = 'left top';
                                        }
                                    }}
                                >
                                    <i className="fas fa-search-plus"></i>
                                </button>
                                <button 
                                    className="px-3 py-2 text-gray-600 hover:bg-gray-50" 
                                    title="Zoom Out"
                                    onClick={() => {
                                        const container = document.querySelector('.ownership-tree-visualization');
                                        if (container) {
                                            // Get current scale
                                            const currentTransform = container.style.transform;
                                            const currentScale = currentTransform ? 
                                                parseFloat(currentTransform.replace(/[^0-9.]/g, '')) || 1 : 1;
                                            
                                            // Apply new scale
                                            container.style.transform = `scale(${currentScale / 1.2})`;
                                            container.style.transformOrigin = 'left top';
                                        }
                                    }}
                                >
                                    <i className="fas fa-search-minus"></i>
                                </button>
                                <button 
                                    className="px-3 py-2 border-l border-gray-200 text-gray-600 hover:bg-gray-50" 
                                    title="Reset Zoom"
                                    onClick={() => {
                                        const container = document.querySelector('.ownership-tree-visualization');
                                        if (container) {
                                            container.style.transform = 'scale(1)';
                                            container.style.transformOrigin = 'left top';
                                        }
                                    }}
                                >
                                    <i className="fas fa-sync-alt"></i>
                                </button>
                            </div>
                            
                            {/* Export/Print Button */}
                            <div className="absolute top-4 right-4 z-10">
                                <button 
                                    className="px-4 py-2 bg-white rounded-md shadow-md text-gray-700 hover:bg-gray-50 flex items-center"
                                    title="Export PNG"
                                    onClick={() => {
                                        alert('Export functionality would be implemented here');
                                        // In a real implementation, we would use html2canvas or similar
                                        // to capture the tree visualization as an image
                                    }}
                                >
                                    <i className="fas fa-download mr-2"></i>
                                    Export PNG
                                </button>
                            </div>
                        </div>
                    ) : (
                        <div className="flex justify-center items-center h-full">
                            <div className="text-center text-gray-500 bg-white rounded-lg shadow-md p-12">
                                <i className="fas fa-sitemap text-5xl mb-4 text-green-800 opacity-50"></i>
                                <p className="text-xl">No ownership structure data available</p>
                                <p className="text-sm mt-2">Upload an ownership file to see the hierarchy</p>
                                <ReactRouterDOM.Link 
                                    to="/upload" 
                                    className="mt-4 inline-flex items-center px-4 py-2 border border-green-800 text-sm font-medium rounded-md text-white bg-green-800 hover:bg-green-700 focus:outline-none"
                                >
                                    <i className="fas fa-upload mr-2"></i>
                                    Upload Ownership Data
                                </ReactRouterDOM.Link>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};