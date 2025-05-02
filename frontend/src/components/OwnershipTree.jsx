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
               (node.accounts && node.accounts.length > 0) ||
               (node.portfolios && node.portfolios.length > 0);
    };
    
    // Generate a unique ID for a node based on its path and entity_id if available
    const getNodeId = (node, parentPath = '') => {
        const entityId = node.entity_id ? node.entity_id : '';
        return `${parentPath}-${entityId}-${node.name || 'unnamed'}`.replace(/\s+/g, '_');
    };
    
    // Determine node type based on level or node properties
    const getNodeType = (node, level) => {
        if (node.type) return node.type;
        
        if (level === 0) return 'client';
        if (level === 1) return 'group';
        if (level === 2) return 'portfolio';
        return 'account';
    };
    
    // Get text for the entity type label
    const getEntityTypeLabel = (type) => {
        switch (type) {
            case 'client': return 'Client';
            case 'group': return 'Group';
            case 'portfolio': return 'Portfolio';
            case 'account': return 'Account';
            default: return 'Entity';
        }
    };
    
    // Format currency value for display
    const formatCurrency = (value) => {
        if (!value) return '';
        
        // Convert to number if it's a string
        let numValue = typeof value === 'string' ? parseFloat(value) : value;
        
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }).format(numValue);
    };
    
    // Render a single node and its children in a horizontal tree layout
    const renderNode = (node, level = 0, parentPath = '', isFirstChild = true, isLastChild = true) => {
        if (!node) return null;
        
        const nodeId = getNodeId(node, parentPath);
        const isExpanded = expandedNodes[nodeId] !== false; // Default to expanded
        const nodeType = getNodeType(node, level);
        const hasNodeChildren = hasChildren(node);
        const entityTypeLabel = getEntityTypeLabel(nodeType);
        
        // Format value if present
        const valueDisplay = node.value ? formatCurrency(node.value) : 
                              node.adjusted_value ? formatCurrency(node.adjusted_value) : '';
        
        // All potential children
        const allChildren = [
            ...(node.children || []),
            ...(node.groups || []),
            ...(node.accounts || []),
            ...(node.portfolios || [])
        ];
        
        return (
            <div key={nodeId} className="tree-node-container">
                {/* Main node card */}
                <div 
                    className={`
                        node-card bg-white border rounded-md shadow-sm overflow-hidden
                        ${hasNodeChildren ? 'cursor-pointer' : ''}
                    `}
                    onClick={() => hasNodeChildren && toggleNode(nodeId)}
                >
                    <div className="p-3">
                        {/* Entity name */}
                        <div className="font-medium text-gray-800 mb-1 truncate max-w-xs">{node.name || 'Unnamed'}</div>
                        
                        {/* Entity type and info */}
                        <div className="text-xs text-gray-500">
                            <div>{entityTypeLabel}</div>
                            {valueDisplay && (
                                <div className="font-medium text-green-800 mt-1">
                                    Adjusted Value: {valueDisplay}
                                </div>
                            )}
                        </div>
                        
                        {/* Expand/collapse indicator for nodes with children */}
                        {hasNodeChildren && (
                            <div className="absolute top-2 right-2 text-gray-400">
                                <i className={`fas fa-chevron-${isExpanded ? 'down' : 'right'}`}></i>
                            </div>
                        )}
                    </div>
                </div>
                
                {/* Children container with connecting lines */}
                {isExpanded && hasNodeChildren && allChildren.length > 0 && (
                    <div className="children-container relative mt-8">
                        {/* Vertical line from parent to children */}
                        <div className="absolute left-1/2 -top-8 h-8 w-px bg-gray-300 -translate-x-1/2"></div>
                        
                        {/* Horizontal line above children */}
                        {allChildren.length > 1 && (
                            <div className="absolute top-0 left-0 right-0 h-px bg-gray-300" 
                                 style={{ 
                                    left: `calc(${100 / allChildren.length / 2}%)`, 
                                    right: `calc(${100 / allChildren.length / 2}%)` 
                                 }}>
                            </div>
                        )}
                        
                        {/* Render all children in a horizontal row */}
                        <div className="flex justify-center gap-8 pt-8">
                            {allChildren.map((child, index) => (
                                <div key={`${nodeId}-child-${index}`} className="relative">
                                    {/* Vertical line to child */}
                                    <div className="absolute left-1/2 -top-8 h-8 w-px bg-gray-300 -translate-x-1/2"></div>
                                    
                                    {/* Render child node */}
                                    {renderNode(
                                        child, 
                                        level + 1, 
                                        nodeId, 
                                        index === 0, 
                                        index === allChildren.length - 1
                                    )}
                                </div>
                            ))}
                        </div>
                    </div>
                )}
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
        <div className="h-full ownership-tree-container relative overflow-hidden">
            {/* Custom CSS for the tree visualization */}
            <style jsx="true">{`
                .ownership-tree-visualization {
                    min-height: 100%;
                    padding: 2rem;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                }
                
                .tree-node-container {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    margin-bottom: 1rem;
                }
                
                .node-card {
                    min-width: 200px;
                    position: relative;
                    transition: all 0.2s ease;
                }
                
                .node-card:hover {
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                }
                
                .children-container {
                    min-width: 100%;
                }
            `}</style>
            
            <div className="ownership-tree-visualization overflow-auto h-full">
                {processedData.map((rootNode, index) => (
                    <div key={`root-${index}`} className="flex flex-col items-center mb-8">
                        {renderNode(rootNode)}
                    </div>
                ))}
            </div>
        </div>
    );
};