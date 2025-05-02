// Professional Tree visualization component for ownership structure
const OwnershipTree = ({ data }) => {
    const [expandedNodes, setExpandedNodes] = React.useState({});
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState(null);
    
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
        if (level === 2) return 'portfolio';
        return 'account';
    };
    
    // Get background color based on node type
    const getBgColorClass = (type) => {
        switch (type) {
            case 'client': return 'bg-green-800';
            case 'group': return 'bg-indigo-500';
            case 'portfolio': return 'bg-purple-500';
            case 'account': return 'bg-gray-500';
            default: return 'bg-gray-400';
        }
    };
    
    // Get icon class based on node type
    const getIconClass = (type) => {
        switch (type) {
            case 'client': return 'user-tie';
            case 'group': return 'folder';
            case 'portfolio': return 'briefcase';
            case 'account': return 'university';
            default: return 'circle';
        }
    };
    
    // Format value for display
    const formatValue = (value) => {
        if (!value) return '';
        
        if (value >= 1000000) {
            return `$${(value / 1000000).toFixed(1)}M`;
        }
        if (value >= 1000) {
            return `$${(value / 1000).toFixed(1)}K`;
        }
        return `$${value}`;
    };
    
    // Render a single node and its children
    const renderNode = (node, level = 0, parentPath = '') => {
        if (!node) return null;
        
        const nodeId = getNodeId(node, parentPath);
        const isExpanded = expandedNodes[nodeId] !== false; // Default to expanded
        const nodeType = getNodeType(node, level);
        const hasNodeChildren = hasChildren(node);
        const iconClass = getIconClass(nodeType);
        const bgColorClass = getBgColorClass(nodeType);
        
        // Format value if present
        const valueDisplay = node.value ? formatValue(node.value) : 
                            node.adjusted_value ? formatValue(node.adjusted_value) : '';
        
        return (
            <div key={nodeId} className="ownership-node relative">
                {/* Connection lines */}
                {level > 0 && (
                    <div className="absolute left-6 -top-6 h-6 w-px bg-gray-300"></div>
                )}
                
                {/* Node container */}
                <div className="relative ml-6">
                    {/* Horizontal line to node */}
                    {level > 0 && (
                        <div className="absolute left-0 top-6 w-6 h-px bg-gray-300 -translate-x-6"></div>
                    )}
                    
                    {/* Node content box */}
                    <div className={`
                        p-3 rounded-md shadow-sm border border-gray-200 mb-1 ml-2
                        ${hasNodeChildren ? 'cursor-pointer' : ''}
                        ${level === 0 ? 'bg-gray-50' : 'bg-white'}
                    `}>
                        <div 
                            className="flex items-center"
                            onClick={() => hasNodeChildren && toggleNode(nodeId)}
                        >
                            {/* Node icon and expanded/collapsed indicator */}
                            <div className={`
                                w-8 h-8 rounded-full flex items-center justify-center mr-3
                                ${bgColorClass} text-white
                            `}>
                                <i className={`fas fa-${iconClass}`}></i>
                            </div>
                            
                            <div className="flex-grow">
                                {/* Node name */}
                                <div className="font-medium text-gray-800">{node.name || 'Unnamed'}</div>
                                
                                {/* Node type and value */}
                                <div className="flex text-xs text-gray-500 mt-1">
                                    <div className="capitalize">{nodeType}</div>
                                    {valueDisplay && (
                                        <div className="ml-3 font-medium">{valueDisplay}</div>
                                    )}
                                </div>
                            </div>
                            
                            {/* Expand/collapse button for nodes with children */}
                            {hasNodeChildren && (
                                <div className="ml-2 text-gray-400">
                                    <i className={`fas fa-chevron-${isExpanded ? 'down' : 'right'}`}></i>
                                </div>
                            )}
                        </div>
                    </div>
                    
                    {/* Children container, indented and with connection lines */}
                    {isExpanded && hasNodeChildren && (
                        <div className="children pl-8 relative">
                            {/* Vertical connection line for multiple children */}
                            {((node.children && node.children.length > 1) || 
                              (node.groups && node.groups.length > 1) ||
                              (node.accounts && node.accounts.length > 1)) && (
                                <div className="absolute left-6 top-0 bottom-6 w-px bg-gray-300"></div>
                            )}
                            
                            {/* Render all types of children */}
                            {node.children?.map(child => 
                                renderNode(child, level + 1, nodeId)
                            )}
                            
                            {node.groups?.map(group => 
                                renderNode(group, level + 1, nodeId)
                            )}
                            
                            {node.accounts?.map(account => 
                                renderNode(account, level + 1, nodeId)
                            )}
                            
                            {node.portfolios?.map(portfolio => 
                                renderNode(portfolio, level + 1, nodeId)
                            )}
                        </div>
                    )}
                </div>
            </div>
        );
    };
    
    const processedData = processData();
    
    // Loading state
    if (loading) {
        return (
            <div className="flex justify-center items-center h-full">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800 mx-auto"></div>
                    <p className="mt-4 text-gray-600">Rendering ownership visualization...</p>
                </div>
            </div>
        );
    }
    
    // Error state
    if (error) {
        return (
            <div className="bg-red-50 p-4 rounded-md text-red-600">
                <i className="fas fa-exclamation-circle mr-2"></i>
                {error}
            </div>
        );
    }
    
    // Empty state
    if (!processedData.length) {
        return (
            <div className="flex justify-center items-center h-full bg-white">
                <div className="text-center text-gray-500">
                    <i className="fas fa-sitemap text-4xl mb-4"></i>
                    <p>No ownership structure data available.</p>
                    <p className="text-sm mt-2">Upload an ownership file to see the hierarchy.</p>
                </div>
            </div>
        );
    }
    
    return (
        <div className="h-full ownership-tree-container relative">
            <div className="ownership-tree-visualization h-full overflow-auto p-4">
                {processedData.map(rootNode => renderNode(rootNode))}
            </div>
            
            {/* Legend - helps users understand node colors */}
            <div className="absolute bottom-4 right-4 bg-white p-2 rounded-md shadow-md text-xs flex gap-3 z-10">
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-green-800 mr-1"></span>
                    <span>Client</span>
                </div>
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-indigo-500 mr-1"></span>
                    <span>Group</span>
                </div>
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-purple-500 mr-1"></span>
                    <span>Portfolio</span>
                </div>
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-gray-500 mr-1"></span>
                    <span>Account</span>
                </div>
            </div>
        </div>
    );
};