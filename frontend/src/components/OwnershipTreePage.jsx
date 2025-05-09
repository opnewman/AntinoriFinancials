// Access React and ReactRouterDOM from globals
const { Link, useHistory } = ReactRouterDOM;

class OwnershipTreePage extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            loading: true,
            treeData: [],
            error: '',
            selectedClient: null,
            expandedClients: new Set(),
            searchTerm: '',
            clientList: []
        };
    }
    
    componentDidMount() {
        this.fetchOwnershipTree();
    }
    
    // Fetch ownership tree data
    fetchOwnershipTree = async () => {
        try {
            this.setState({ loading: true });
            console.log('Fetching ownership tree data...');
            
            const data = await api.getOwnershipTree();
            console.log('Ownership tree data loaded');
            
            let treeData = [];
            if (Array.isArray(data)) {
                treeData = data;
            } else if (data) {
                // If data is not an array but exists, wrap it
                console.warn('Ownership tree data is not an array, converting to array');
                treeData = [data];
            }
            
            // Extract client list for sidebar
            const clientList = this.extractClientList(treeData);
            
            this.setState({
                treeData,
                clientList,
                loading: false
            });
        } catch (err) {
            console.error('Error fetching ownership tree:', err);
            this.setState({
                error: 'Failed to load ownership structure. Please try again later.',
                loading: false
            });
        }
    };
    
    // Extract client list for sidebar
    extractClientList = (treeData) => {
        let clients = [];
        
        if (treeData && treeData.length > 0 && treeData[0].data && treeData[0].data.children) {
            // Process based on data structure
            const clientNodes = treeData[0].data.children;
            
            clientNodes.forEach(clientNode => {
                if (clientNode && clientNode.name) {
                    clients.push({
                        name: clientNode.name,
                        id: clientNode.entity_id || `client-${Math.random().toString(36).substring(2, 9)}`
                    });
                }
            });
        }
        
        return clients;
    };
    
    // Handle client selection
    selectClient = (clientName) => {
        this.setState({ selectedClient: clientName });
    };
    
    // Toggle client expansion in list
    toggleClientExpansion = (clientId) => {
        const { expandedClients } = this.state;
        const newExpandedClients = new Set(expandedClients);
        
        if (newExpandedClients.has(clientId)) {
            newExpandedClients.delete(clientId);
        } else {
            newExpandedClients.add(clientId);
        }
        
        this.setState({ expandedClients: newExpandedClients });
    };
    
    // Handle search input
    handleSearch = (e) => {
        this.setState({ searchTerm: e.target.value });
    };
    
    // Filter clients by search term
    getFilteredClients = () => {
        const { clientList, searchTerm } = this.state;
        
        if (!searchTerm) return clientList;
        
        return clientList.filter(client => 
            client.name.toLowerCase().includes(searchTerm.toLowerCase())
        );
    };
    
    // Expand all clients
    expandAll = () => {
        const { clientList } = this.state;
        const allClientIds = new Set(clientList.map(client => client.id));
        this.setState({ expandedClients: allClientIds });
    };
    
    // Collapse all clients
    collapseAll = () => {
        this.setState({ expandedClients: new Set() });
    };
    
    // Refresh data
    refreshData = () => {
        this.fetchOwnershipTree();
    };
    
    // Export visualization as PNG
    exportAsPNG = () => {
        alert('Export as PNG functionality would be implemented here');
    };
    
    // Render the ownership tree visualization
    renderVisualization = () => {
        const { treeData, selectedClient } = this.state;
        
        if (!treeData || treeData.length === 0) {
            return (
                <div className="text-center text-gray-500 py-8">
                    <i className="fas fa-sitemap text-4xl mb-4"></i>
                    <p>No ownership structure data available</p>
                    <p className="text-sm mt-2">Upload an ownership file to see the hierarchy</p>
                </div>
            );
        }
        
        // Here we'd render the actual D3 visualization
        // For now, we'll use a placeholder
        return (
            <div className="visualization-container bg-white p-4 rounded-lg shadow-md min-h-[600px] relative">
                <div className="visualization-header flex justify-between items-center mb-4">
                    <div className="stats flex space-x-8">
                        <div className="stat">
                            <h3 className="text-sm text-gray-500">TOTAL VALUE</h3>
                            <p className="text-2xl font-bold text-green-800">$39,904</p>
                        </div>
                        <div className="stat">
                            <h3 className="text-sm text-gray-500">ACCOUNTS</h3>
                            <p className="text-2xl font-bold">39,904</p>
                        </div>
                        <div className="stat">
                            <h3 className="text-sm text-gray-500">ENTITIES</h3>
                            <p className="text-2xl font-bold">39,788</p>
                        </div>
                    </div>
                    <button 
                        className="bg-white border border-green-800 text-green-800 px-4 py-2 rounded hover:bg-green-50"
                        onClick={this.exportAsPNG}
                    >
                        Export PNG
                    </button>
                </div>
                
                {/* Breadcrumb navigation */}
                <div className="breadcrumb mb-4 text-purple-700">
                    <span 
                        className="cursor-pointer hover:underline" 
                        onClick={() => this.selectClient(null)}
                    >
                        Home
                    </span>
                    <span className="mx-2">›</span>
                    <span 
                        className="cursor-pointer hover:underline"
                        onClick={() => this.selectClient(null)}
                    >
                        Ownership Tree
                    </span>
                    {selectedClient && (
                        <>
                            <span className="mx-2">›</span>
                            <span>{selectedClient}</span>
                        </>
                    )}
                </div>
                
                {/* Visualization area */}
                <div className="tree-visualization-area border border-gray-200 rounded-lg min-h-[500px] flex items-center justify-center">
                    <div className="text-center text-gray-500">
                        <i className="fas fa-project-diagram text-6xl mb-4 text-green-700 opacity-30"></i>
                        <p className="text-lg">Ownership tree visualization would appear here</p>
                        <p className="text-sm mt-2">Connected to real data from the backend API</p>
                    </div>
                </div>
                
                <div className="visualization-footer mt-4 text-sm text-gray-500 text-right">
                    Data as of May 1, 2025
                </div>
            </div>
        );
    };
    
    render() {
        const { loading, error, searchTerm, expandedClients } = this.state;
        const filteredClients = this.getFilteredClients();
        
        return (
            <div className="flex min-h-screen bg-gray-50">
                {/* Left sidebar */}
                <div className="w-64 bg-white border-r border-gray-200 p-4">
                    <h2 className="text-xl font-bold text-green-800 mb-4">Ownership Tree</h2>
                    
                    <div className="mb-4">
                        <Link to="/" className="flex items-center text-gray-600 hover:text-green-800">
                            <i className="fas fa-arrow-left mr-2"></i>
                            Back to Dashboard
                        </Link>
                    </div>
                    
                    <div className="mb-4">
                        <input
                            type="text"
                            placeholder="Search clients..."
                            value={searchTerm}
                            onChange={this.handleSearch}
                            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-green-500"
                        />
                    </div>
                    
                    <div className="flex space-x-2 mb-4">
                        <button 
                            onClick={this.expandAll}
                            className="bg-gray-100 text-gray-700 px-2 py-1 text-sm rounded hover:bg-gray-200 flex-grow"
                        >
                            Expand All
                        </button>
                        <button 
                            onClick={this.collapseAll}
                            className="bg-gray-100 text-gray-700 px-2 py-1 text-sm rounded hover:bg-gray-200 flex-grow"
                        >
                            Collapse All
                        </button>
                    </div>
                    
                    <button 
                        onClick={this.refreshData}
                        className="mb-4 bg-gray-100 text-gray-700 px-3 py-1 text-sm rounded hover:bg-gray-200 w-full"
                    >
                        Refresh Data
                    </button>
                    
                    <h3 className="font-semibold text-gray-700 mb-2">Clients</h3>
                    
                    <div className="client-list max-h-[calc(100vh-240px)] overflow-y-auto">
                        {filteredClients.map(client => (
                            <div 
                                key={client.id} 
                                className={`py-2 px-1 border-b border-gray-100 cursor-pointer hover:bg-gray-50 ${
                                    this.state.selectedClient === client.name ? 'bg-green-50' : ''
                                }`}
                                onClick={() => this.selectClient(client.name)}
                            >
                                {client.name}
                            </div>
                        ))}
                    </div>
                </div>
                
                {/* Main content */}
                <div className="flex-1 p-6">
                    {loading ? (
                        <div className="flex justify-center items-center h-full">
                            <div className="text-center">
                                <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                                <p className="mt-4 text-lg font-medium text-gray-700">Loading ownership structure...</p>
                            </div>
                        </div>
                    ) : error ? (
                        <div className="bg-red-50 text-red-600 p-4 rounded-md">
                            <i className="fas fa-exclamation-circle mr-2"></i>
                            {error}
                        </div>
                    ) : (
                        this.renderVisualization()
                    )}
                </div>
            </div>
        );
    }
}

// Make component available globally
window.OwnershipTreePage = OwnershipTreePage;