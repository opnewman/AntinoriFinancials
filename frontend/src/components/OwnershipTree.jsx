// Professional Tree visualization component for ownership structure - Left to Right layout
const OwnershipTree = ({ data }) => {
    const [expandedNodes, setExpandedNodes] = React.useState({});
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState(null);
    const containerRef = React.useRef(null);
    
    // Process and prepare data for display
    const processData = () => {
        if (!data) return null;
        
        // Handle different data formats
        if (typeof data === 'object' && !Array.isArray(data)) {
            // Single object (like {name: "All Clients", children: [...]})
            return data;
        }
        
        // If it's an array and has only one item, return that item
        if (Array.isArray(data) && data.length === 1) {
            return data[0];
        }
        
        // If it's an array with multiple items, wrap them in a root node
        if (Array.isArray(data) && data.length > 1) {
            return {
                name: "All Entities",
                children: data
            };
        }
        
        return null;
    };
    
    // Toggle node expansion
    const toggleNode = (nodeId) => {
        setExpandedNodes(prev => ({
            ...prev,
            [nodeId]: !prev[nodeId]
        }));
    };
    
    // Check if node is expanded
    const isNodeExpanded = (nodeId) => {
        // If node expansion state is explicitly set, use that
        if (expandedNodes.hasOwnProperty(nodeId)) {
            return expandedNodes[nodeId];
        }
        // Default behavior: expand first level, collapse others
        return nodeId.split('-').length <= 2; // Only root and its immediate children expanded by default
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
        return `${parentPath}-${entityId}-${(node.name || 'unnamed').replace(/[^a-zA-Z0-9]/g, '_')}`;
    };
    
    // Determine node type based on properties and structure
    const getNodeType = (node, level) => {
        // If node has type property, use that
        if (node.type) return node.type;
        
        // If node has account_number property, it's an account
        if (node.account_number) return 'account';
        
        // Use level as a fallback
        if (level === 0) return 'client';
        if (level === 1) return 'group';
        if (level === 2) return 'portfolio';
        
        // Default to account for leaf nodes
        return 'account';
    };
    
    // Get node color based on node type
    const getNodeColor = (type) => {
        switch (type) {
            case 'client': return '#14532D'; // Dark green
            case 'group': return '#4F46E5';  // Indigo
            case 'portfolio': return '#7C3AED'; // Purple
            case 'account': return '#64748B'; // Gray
            default: return '#94A3B8';
        }
    };
    
    // Format currency value for display
    const formatCurrency = (value) => {
        if (value === undefined || value === null) return '';
        if (value === 0 || value === '0') return '$0';
        
        // Convert to number if it's a string
        const numValue = typeof value === 'string' ? parseFloat(value.replace(/[^0-9.-]+/g, '')) : value;
        
        if (isNaN(numValue)) return '';
        
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            maximumFractionDigits: 0
        }).format(numValue);
    };
    
    // Toggle expand/collapse all nodes
    const toggleAllNodes = (expand = true) => {
        if (expand) {
            // Find all possible nodes and expand them
            const allNodes = {};
            
            const findAllNodes = (node, parentPath = '') => {
                if (!node) return;
                
                const nodeId = getNodeId(node, parentPath);
                allNodes[nodeId] = true;
                
                // Process children
                if (node.children) {
                    node.children.forEach(child => findAllNodes(child, nodeId));
                }
                
                // Process groups
                if (node.groups) {
                    node.groups.forEach(group => findAllNodes(group, nodeId));
                }
                
                // Process accounts
                if (node.accounts) {
                    node.accounts.forEach(account => findAllNodes(account, nodeId));
                }
                
                // Process portfolios
                if (node.portfolios) {
                    node.portfolios.forEach(portfolio => findAllNodes(portfolio, nodeId));
                }
            };
            
            findAllNodes(processedData);
            setExpandedNodes(allNodes);
        } else {
            // Collapse all except root
            const rootId = getNodeId(processedData);
            setExpandedNodes({ [rootId]: true });
        }
    };
    
    // Render a node with left-to-right tree layout
    const renderLeftToRightTree = () => {
        if (!processedData) return null;
        
        // Create function to recursively render nodes
        const renderNode = (node, level = 0, parentPath = '') => {
            if (!node) return null;
            
            const nodeId = getNodeId(node, parentPath);
            const nodeType = getNodeType(node, level);
            const nodeColor = getNodeColor(nodeType);
            const isExpanded = isNodeExpanded(nodeId);
            
            // Get all children from different properties
            const allChildren = [
                ...(node.children || []),
                ...(node.groups || []),
                ...(node.accounts || []),
                ...(node.portfolios || [])
            ];
            
            const hasNodeChildren = allChildren.length > 0;
            
            // Format value for display
            const value = node.value || node.adjusted_value || 0;
            const formattedValue = formatCurrency(value);
            
            // Skip the root node if it's just a container
            if (level === 0 && node.name === "All Entities") {
                return (
                    <div className="tree-root">
                        {allChildren.map((child, index) => 
                            renderNode(child, level + 1, nodeId)
                        )}
                    </div>
                );
            }
            
            return (
                <div key={nodeId} className="node-row">
                    {/* Node box */}
                    <div 
                        className={`node-box ${hasNodeChildren ? 'has-children' : ''} ${nodeType}`}
                        onClick={() => hasNodeChildren && toggleNode(nodeId)}
                    >
                        {/* Node indicator circle */}
                        <div className="node-indicator" style={{ backgroundColor: nodeColor }}></div>
                        
                        {/* Node content */}
                        <div className="node-content">
                            <div className="node-name">{node.name}</div>
                            <div className="node-type">{nodeType}</div>
                            {value > 0 && (
                                <div className="node-value">{formattedValue}</div>
                            )}
                        </div>
                        
                        {/* Expand/collapse icon */}
                        {hasNodeChildren && (
                            <div className="expand-icon">
                                <i className={`fas fa-chevron-${isExpanded ? 'down' : 'right'}`}></i>
                            </div>
                        )}
                    </div>
                    
                    {/* Children container */}
                    {isExpanded && hasNodeChildren && (
                        <div className="children-container">
                            {allChildren.map((child, index) => 
                                renderNode(child, level + 1, nodeId)
                            )}
                        </div>
                    )}
                </div>
            );
        };
        
        return renderNode(processedData);
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
    if (!processedData) {
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
        <div className="h-full ownership-tree-container relative overflow-hidden" ref={containerRef}>
            {/* Custom CSS for the left-to-right tree visualization */}
            <style jsx="true">{`
                .ownership-tree-container {
                    position: relative;
                }
                
                .ownership-tree-visualization {
                    position: absolute;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    overflow: auto;
                    padding: 20px;
                }
                
                .tree-root {
                    display: flex;
                    flex-direction: column;
                    gap: 20px;
                    margin-left: 20px;
                }
                
                .node-row {
                    display: flex;
                    align-items: flex-start;
                    margin-bottom: 15px;
                    position: relative;
                }
                
                .node-box {
                    display: flex;
                    align-items: center;
                    padding: 8px 15px;
                    background: white;
                    border: 1px solid #e2e8f0;
                    border-radius: 6px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                    margin-right: 30px;
                    position: relative;
                    min-width: 180px;
                }
                
                .node-box.has-children {
                    cursor: pointer;
                }
                
                .node-box.has-children:hover {
                    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
                }
                
                .node-box::after {
                    content: '';
                    position: absolute;
                    top: 50%;
                    right: -30px;
                    width: 30px;
                    height: 1px;
                    background-color: #cbd5e1;
                    display: none;
                }
                
                .node-box.has-children::after {
                    display: block;
                }
                
                .node-indicator {
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    margin-right: 10px;
                    flex-shrink: 0;
                }
                
                .node-content {
                    flex-grow: 1;
                    overflow: hidden;
                }
                
                .node-name {
                    font-weight: 500;
                    color: #1e293b;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                }
                
                .node-type {
                    font-size: 0.7rem;
                    color: #64748b;
                    text-transform: capitalize;
                }
                
                .node-value {
                    font-size: 0.8rem;
                    font-weight: 500;
                    color: #14532d;
                    margin-top: 2px;
                }
                
                .expand-icon {
                    color: #94a3b8;
                    margin-left: 10px;
                    font-size: 0.8rem;
                }
                
                .children-container {
                    position: relative;
                    display: flex;
                    flex-direction: column;
                }
                
                .children-container::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: -15px;
                    width: 1px;
                    height: calc(100% - 7px);
                    background-color: #cbd5e1;
                }
                
                .children-container .node-row:last-child .children-container::before {
                    height: 20px;
                }
                
                .children-container .node-row::before {
                    content: '';
                    position: absolute;
                    top: 18px;
                    left: -15px;
                    width: 15px;
                    height: 1px;
                    background-color: #cbd5e1;
                }
                
                /* Make client nodes stand out */
                .node-box.client .node-name {
                    font-weight: 600;
                }
                
                /* Control buttons */
                .tree-controls {
                    position: absolute;
                    top: 10px;
                    right: 10px;
                    z-index: 10;
                    display: flex;
                    gap: 5px;
                }
                
                .control-button {
                    background: white;
                    border: 1px solid #e2e8f0;
                    border-radius: 4px;
                    padding: 5px 8px;
                    font-size: 0.8rem;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #475569;
                    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
                }
                
                .control-button:hover {
                    background: #f8fafc;
                    color: #1e293b;
                }
                
                .control-button i {
                    margin-right: 4px;
                }
            `}</style>
            
            <div className="ownership-tree-visualization">
                {/* Tree controls */}
                <div className="tree-controls">
                    <button 
                        className="control-button" 
                        onClick={() => toggleAllNodes(true)}
                        title="Expand All"
                    >
                        <i className="fas fa-expand-alt"></i>
                    </button>
                    <button 
                        className="control-button" 
                        onClick={() => toggleAllNodes(false)}
                        title="Collapse All"
                    >
                        <i className="fas fa-compress-alt"></i>
                    </button>
                </div>
                
                {/* Tree visualization */}
                {renderLeftToRightTree()}
            </div>
        </div>
    );
};