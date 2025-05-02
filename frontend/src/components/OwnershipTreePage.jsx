const OwnershipTreePage = () => {
    const [loading, setLoading] = React.useState(true);
    const [treeData, setTreeData] = React.useState([]);
    const [error, setError] = React.useState('');
    
    // Fetch ownership tree data on component mount
    React.useEffect(() => {
        const fetchOwnershipTree = async () => {
            try {
                setLoading(true);
                console.log('Fetching ownership tree data...');
                
                const data = await api.getOwnershipTree();
                if (Array.isArray(data)) {
                    setTreeData(data);
                } else if (data) {
                    // If data is not an array but exists, wrap it
                    setTreeData([data]);
                } else {
                    setTreeData([]);
                    setError('No ownership structure data available');
                }
                
                console.log('Ownership tree data loaded');
            } catch (err) {
                console.error('Error fetching ownership tree:', err);
                setError('Failed to load ownership structure. Please try again later.');
                setTreeData([]);
            } finally {
                setLoading(false);
            }
        };
        
        fetchOwnershipTree();
    }, []);
    
    // Render tree node
    const renderTreeNode = (node, level = 0) => {
        // Generate unique key for node
        const nodeKey = `${node.name}-${level}-${Math.random().toString(36).substr(2, 9)}`;
        
        // Style based on level
        const levelClass = level === 0 ? 'text-green-800 font-bold' : 
                          level === 1 ? 'text-blue-700' : 
                          level === 2 ? 'text-indigo-600' : 'text-gray-700';
        
        // Icon based on level
        const iconClass = level === 0 ? 'fa-user' : 
                         level === 1 ? 'fa-folder' : 'fa-file-alt';
        
        return (
            <div key={nodeKey} className="tree-node" style={{ marginLeft: `${level * 20}px` }}>
                <div className={`flex items-center py-2 ${levelClass}`}>
                    <i className={`fas ${iconClass} mr-2`}></i>
                    <span>{node.name}</span>
                </div>
                
                {/* Render child groups */}
                {node.groups && node.groups.length > 0 && (
                    <div className="groups">
                        {node.groups.map(group => renderTreeNode(group, level + 1))}
                    </div>
                )}
                
                {/* Render accounts */}
                {node.accounts && node.accounts.length > 0 && (
                    <div className="accounts">
                        {node.accounts.map(account => renderTreeNode(account, level + 1))}
                    </div>
                )}
                
                {/* Handle alternate structure where data.children exists */}
                {node.children && node.children.length > 0 && (
                    <div className="children">
                        {node.children.map(child => renderTreeNode(child, level + 1))}
                    </div>
                )}
            </div>
        );
    };
    
    if (loading) {
        return (
            <div className="container mx-auto p-4">
                <div className="bg-white rounded-lg shadow-md p-6 text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                    <p className="mt-4 text-lg font-medium text-gray-700">Loading ownership structure...</p>
                </div>
            </div>
        );
    }
    
    return (
        <div className="container mx-auto p-4">
            <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-bold mb-4">Ownership Structure</h2>
                
                {error && (
                    <div className="bg-red-50 text-red-600 p-3 rounded-md mb-4">
                        <i className="fas fa-exclamation-circle mr-2"></i>
                        {error}
                    </div>
                )}
                
                <div className="mt-4 ownership-tree bg-gray-50 p-4 rounded-lg max-h-screen overflow-y-auto">
                    {treeData.length > 0 ? (
                        treeData.map(node => renderTreeNode(node))
                    ) : (
                        <div className="text-center text-gray-500 py-8">
                            <i className="fas fa-sitemap text-4xl mb-4"></i>
                            <p>No ownership structure data available</p>
                            <p className="text-sm mt-2">Upload an ownership file to see the hierarchy</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};