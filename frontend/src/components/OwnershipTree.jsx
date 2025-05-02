// Tree visualization component for ownership structure
const OwnershipTree = ({ data }) => {
    const [expandedNodes, setExpandedNodes] = React.useState({});
    
    // Toggle node expansion
    const toggleNode = (nodeId) => {
        setExpandedNodes(prev => ({
            ...prev,
            [nodeId]: !prev[nodeId]
        }));
    };
    
    // Check if node has children
    const hasChildren = (node) => {
        return node.groups?.length > 0 || node.accounts?.length > 0;
    };
    
    // Generate a unique ID for a node based on its path
    const getNodeId = (node, parentPath = '') => {
        return `${parentPath}-${node.name}`.replace(/\s+/g, '_');
    };
    
    // Render a single node and its children
    const renderNode = (node, level = 0, parentPath = '') => {
        const nodeId = getNodeId(node, parentPath);
        const isExpanded = expandedNodes[nodeId] !== false; // Default to expanded
        const hasNodeChildren = hasChildren(node);
        
        // Indent based on level
        const indentStyle = {
            paddingLeft: `${level * 20}px`
        };
        
        return (
            <div key={nodeId} className="ownership-node">
                <div 
                    className="flex items-center py-1 hover:bg-gray-100 cursor-pointer"
                    style={indentStyle}
                    onClick={() => hasNodeChildren && toggleNode(nodeId)}
                >
                    {/* Expand/collapse icon */}
                    {hasNodeChildren && (
                        <span className="mr-2 text-gray-600">
                            <i className={`fas fa-chevron-${isExpanded ? 'down' : 'right'}`}></i>
                        </span>
                    )}
                    
                    {/* Node icon based on type */}
                    <span className="mr-2 text-green-700">
                        <i className={`fas fa-${level === 0 ? 'user' : level === 1 ? 'folder' : 'file-alt'}`}></i>
                    </span>
                    
                    {/* Node name */}
                    <span className="font-medium">{node.name}</span>
                </div>
                
                {/* Children (if expanded) */}
                {isExpanded && hasNodeChildren && (
                    <div className="children">
                        {/* Render groups */}
                        {node.groups?.map(group => 
                            renderNode(group, level + 1, nodeId)
                        )}
                        
                        {/* Render accounts */}
                        {node.accounts?.map(account => 
                            renderNode(account, level + 1, nodeId)
                        )}
                    </div>
                )}
            </div>
        );
    };
    
    // Empty state
    if (!data || data.length === 0) {
        return (
            <div className="bg-white rounded-lg shadow-md p-4 text-center text-gray-500">
                <i className="fas fa-sitemap text-2xl mb-2"></i>
                <p>No ownership structure data available.</p>
                <p className="text-sm mt-2">Upload an ownership file to see the hierarchy.</p>
            </div>
        );
    }
    
    return (
        <div className="bg-white rounded-lg shadow-md p-4 ownership-tree">
            <h3 className="text-lg font-semibold mb-3">Ownership Structure</h3>
            <div className="tree-container max-h-96 overflow-y-auto border border-gray-200 rounded p-2">
                {data.map(client => renderNode(client))}
            </div>
        </div>
    );
};