// Professional Tree visualization component for ownership structure
const OwnershipTree = ({ data }) => {
    const containerRef = React.useRef(null);
    const [treeData, setTreeData] = React.useState(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(null);
    
    // Initialize d3 visualization when component mounts or data changes
    React.useEffect(() => {
        if (!data || !containerRef.current) return;
        
        setLoading(true);
        
        try {
            // Process the data to ensure it's in the right format
            const processedData = processData(data);
            setTreeData(processedData);
            
            // Clear previous visualization
            d3.select(containerRef.current).selectAll("*").remove();
            
            // Only proceed if we have valid data
            if (processedData && processedData.length > 0) {
                renderTree(processedData);
            }
        } catch (err) {
            console.error('Error rendering ownership tree:', err);
            setError('Failed to render the ownership visualization');
        } finally {
            setLoading(false);
        }
    }, [data, containerRef.current]);
    
    // Process and prepare data for display
    const processData = (rawData) => {
        if (!rawData) return [];
        
        // Handle different data formats
        if (typeof rawData === 'object' && !Array.isArray(rawData)) {
            // Single object (like {name: "All Clients", children: [...]})
            return rawData;
        }
        
        // If it's an array, wrap it in a parent node
        if (Array.isArray(rawData)) {
            return {
                name: "All Entities",
                children: rawData
            };
        }
        
        return rawData;
    };
    
    // Render the tree visualization using D3
    const renderTree = (treeData) => {
        const container = containerRef.current;
        const width = container.clientWidth;
        const height = container.clientHeight || 800;
        
        // Create SVG container
        const svg = d3.select(container)
            .append("svg")
            .attr("width", width)
            .attr("height", height)
            .append("g")
            .attr("transform", `translate(${width / 2}, 60)`);
        
        // Create tree layout
        const treeLayout = d3.tree()
            .size([width - 160, height - 120]);
        
        // Create hierarchy
        const root = d3.hierarchy(treeData);
        
        // Assign x,y positions to nodes
        treeLayout(root);
        
        // Add links between nodes
        svg.selectAll(".link")
            .data(root.links())
            .enter()
            .append("path")
            .attr("class", "link")
            .attr("d", d => {
                return `M${d.source.x},${d.source.y}
                        C${d.source.x},${(d.source.y + d.target.y) / 2}
                        ${d.target.x},${(d.source.y + d.target.y) / 2}
                        ${d.target.x},${d.target.y}`;
            })
            .attr("fill", "none")
            .attr("stroke", "#d1d5db")
            .attr("stroke-width", 1.5);
        
        // Add nodes
        const nodes = svg.selectAll(".node")
            .data(root.descendants())
            .enter()
            .append("g")
            .attr("class", d => `node ${d.data.type || getNodeTypeByDepth(d.depth)}`)
            .attr("transform", d => `translate(${d.x},${d.y})`);
        
        // Add node circles
        nodes.append("circle")
            .attr("r", 8)
            .attr("fill", d => getNodeColor(d.data.type || getNodeTypeByDepth(d.depth)))
            .attr("stroke", "#fff")
            .attr("stroke-width", 2);
        
        // Add labels
        nodes.append("text")
            .attr("dy", d => d.children ? -15 : 20)
            .attr("text-anchor", "middle")
            .attr("font-size", "0.8rem")
            .text(d => d.data.name)
            .attr("fill", "#374151");
        
        // Add value labels (if they exist)
        nodes.filter(d => d.data.value || d.data.adjusted_value)
            .append("text")
            .attr("dy", 35)
            .attr("text-anchor", "middle")
            .attr("font-size", "0.7rem")
            .attr("fill", "#6B7280")
            .text(d => {
                const value = d.data.value || d.data.adjusted_value;
                return value ? `$${formatNumber(value)}` : '';
            });
        
        // Helper function to get node type based on depth
        function getNodeTypeByDepth(depth) {
            if (depth === 0) return 'client';
            if (depth === 1) return 'group';
            if (depth === 2) return 'portfolio';
            return 'account';
        }
        
        // Helper function to get node color based on type
        function getNodeColor(type) {
            switch(type) {
                case 'client': return '#14532D'; // Primary green
                case 'group': return '#4F46E5';  // Indigo
                case 'portfolio': return '#7C3AED'; // Purple
                case 'account': return '#6B7280'; // Gray
                default: return '#9CA3AF';
            }
        }
        
        // Format number for display (e.g., 1234567 to "1.2M")
        function formatNumber(num) {
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            }
            if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return num.toString();
        }
    };
    
    // Loading state
    if (loading && !treeData) {
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
    if (!data) {
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
        <div className="h-full ownership-tree-container">
            <div 
                ref={containerRef} 
                className="ownership-tree-visualization h-full w-full overflow-auto"
            >
                {/* D3 visualization will be rendered here */}
            </div>
            
            {/* Legend - helps users understand node colors */}
            <div className="absolute bottom-4 right-4 bg-white p-2 rounded-md shadow-md text-xs flex gap-3">
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-green-800 mr-1"></span>
                    <span>Client</span>
                </div>
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-indigo-600 mr-1"></span>
                    <span>Group</span>
                </div>
                <div className="flex items-center">
                    <span className="inline-block w-3 h-3 rounded-full bg-purple-600 mr-1"></span>
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