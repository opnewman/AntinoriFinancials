// Tree visualization component for ownership structure
const OwnershipTree = ({ data }) => {
    const [expandedNodes, setExpandedNodes] = React.useState({});
    const svgRef = React.useRef(null);
    
    // Toggle node expansion
    const toggleNode = (nodeId) => {
        setExpandedNodes(prev => ({
            ...prev,
            [nodeId]: !prev[nodeId]
        }));
    };
    
    // Helper to check if a node has children
    const hasChildren = (node) => {
        return (node.children && node.children.length > 0) || 
               (node.groups && node.groups.length > 0) || 
               (node.accounts && node.accounts.length > 0);
    };
    
    // Generate a unique ID for a node based on its path
    const getNodeId = (node, parentPath = '') => {
        return `${parentPath}-${node.name || 'unnamed'}`.replace(/\s+/g, '_');
    };
    
    // Determine node type based on level or node properties
    const getNodeType = (node, level) => {
        if (node.type) return node.type;
        
        if (level === 0) return 'client';
        if (level === 1) return 'group';
        return 'account';
    };
    
    // Get icon class based on node type
    const getIconClass = (type) => {
        switch (type) {
            case 'client': return 'user';
            case 'group': return 'folder';
            case 'portfolio': return 'briefcase';
            case 'account': return 'file-alt';
            default: return 'circle';
        }
    };
    
    // Get color class based on node type
    const getColorClass = (type) => {
        switch (type) {
            case 'client': return 'text-green-700';
            case 'group': return 'text-blue-600';
            case 'portfolio': return 'text-purple-600';
            case 'account': return 'text-gray-600';
            default: return 'text-gray-500';
        }
    };
    
    // Render a single node and its children
    const renderNode = (node, level = 0, parentPath = '') => {
        if (!node) return null;
        
        const nodeId = getNodeId(node, parentPath);
        const isExpanded = expandedNodes[nodeId] !== false; // Default to expanded
        const nodeType = getNodeType(node, level);
        const hasNodeChildren = hasChildren(node);
        const iconClass = getIconClass(nodeType);
        const colorClass = getColorClass(nodeType);
        
        // Format value if present
        const valueDisplay = node.value ? `($${node.value.toLocaleString()})` : '';
        
        // Indent based on level
        const indentStyle = {
            paddingLeft: `${level * 20}px`
        };
        
        return (
            <div key={nodeId} className="ownership-node">
                <div 
                    className={`flex items-center py-1 hover:bg-gray-100 cursor-pointer ${level === 0 ? 'font-bold' : ''}`}
                    style={indentStyle}
                    onClick={() => hasNodeChildren && toggleNode(nodeId)}
                >
                    {/* Expand/collapse icon */}
                    {hasNodeChildren ? (
                        <span className="mr-2 text-gray-600 w-4">
                            <i className={`fas fa-chevron-${isExpanded ? 'down' : 'right'}`}></i>
                        </span>
                    ) : (
                        <span className="mr-2 w-4"></span>
                    )}
                    
                    {/* Node icon based on type */}
                    <span className={`mr-2 ${colorClass}`}>
                        <i className={`fas fa-${iconClass}`}></i>
                    </span>
                    
                    {/* Node name and value if available */}
                    <span className="font-medium">{node.name || 'Unnamed'}</span>
                    {valueDisplay && (
                        <span className="ml-2 text-gray-500 text-sm">{valueDisplay}</span>
                    )}
                </div>
                
                {/* Children (if expanded) */}
                {isExpanded && hasNodeChildren && (
                    <div className="children">
                        {/* Standard children array */}
                        {node.children?.map(child => 
                            renderNode(child, level + 1, nodeId)
                        )}
                        
                        {/* Groups (if using that structure) */}
                        {node.groups?.map(group => 
                            renderNode(group, level + 1, nodeId)
                        )}
                        
                        {/* Accounts (if using that structure) */}
                        {node.accounts?.map(account => 
                            renderNode(account, level + 1, nodeId)
                        )}
                    </div>
                )}
            </div>
        );
    };
    
    // Process and prepare data for display
    const processData = () => {
        if (!data) return [];
        
        // Handle different data formats
        if (typeof data === 'object' && !Array.isArray(data)) {
            // Single object (like {name: "All Clients", children: [...]})
            return [data];
        }
        
        // Already an array
        return Array.isArray(data) ? data : [];
    };
    
    const processedData = processData();
    
    // Empty state
    if (!processedData.length) {
        return (
            <div className="bg-white rounded-lg shadow-md p-4 text-center text-gray-500">
                <i className="fas fa-sitemap text-4xl mb-4"></i>
                <p>No ownership structure data available.</p>
                <p className="text-sm mt-2">Upload an ownership file to see the hierarchy.</p>
            </div>
        );
    }
    
    return (
        <div className="bg-white rounded-lg shadow-md p-4 ownership-tree">
            <div className="tree-container border border-gray-200 rounded p-2 overflow-y-auto" style={{ maxHeight: 'calc(100vh - 200px)' }}>
                {processedData.map(rootNode => renderNode(rootNode))}
            </div>
        </div>
    );
};