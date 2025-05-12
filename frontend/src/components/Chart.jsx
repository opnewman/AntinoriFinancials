// Basic Chart component class
class ChartComponentClass extends React.Component {
    constructor(props) {
        super(props);
        this.chartRef = React.createRef();
        this.chartInstance = null;
    }
    
    // Default chart options
    getDefaultOptions() {
        return {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        font: {
                            family: 'Inter, sans-serif'
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.8)',
                    titleFont: {
                        family: 'Inter, sans-serif',
                        size: 14
                    },
                    bodyFont: {
                        family: 'Inter, sans-serif',
                        size: 13
                    },
                    padding: 12,
                    cornerRadius: 4,
                    displayColors: true
                }
            }
        };
    }
    
    // Combine default options with provided options
    getChartOptions() {
        return {
            ...this.getDefaultOptions(),
            ...(this.props.options || {})
        };
    }
    
    componentDidMount() {
        this.createChart();
    }
    
    componentDidUpdate(prevProps) {
        // If data or type changed, recreate the chart
        if (prevProps.data !== this.props.data || prevProps.type !== this.props.type) {
            this.destroyChart();
            this.createChart();
        }
    }
    
    componentWillUnmount() {
        this.destroyChart();
    }
    
    destroyChart() {
        try {
            if (this.chartInstance && typeof this.chartInstance.destroy === 'function') {
                this.chartInstance.destroy();
            }
        } catch (error) {
            console.error('Error destroying chart:', error);
        } finally {
            this.chartInstance = null;
        }
    }
    
    createChart() {
        const { data, type } = this.props;
        
        console.log(`🔍 Attempting to create ${type || 'bar'} chart with data:`, 
            typeof data === 'object' ? JSON.stringify(data) : data);
        
        if (!this.chartRef.current) {
            console.error('❌ Chart reference is missing, cannot create chart');
            return;
        }
        
        // Handle missing data with more detailed error messages
        if (!data) {
            console.error('❌ Chart data is null or undefined, cannot create chart');
            return;
        }
        
        // Ensure datasets exist
        if (!data.datasets) {
            console.error('❌ Chart data is missing datasets property');
            return;
        }
        
        if (!Array.isArray(data.datasets) || data.datasets.length === 0) {
            console.error('❌ Chart data has empty or invalid datasets array');
            return;
        }
        
        // Ensure labels exist
        if (!data.labels) {
            console.error('❌ Chart data is missing labels property');
            return;
        }
        
        if (!Array.isArray(data.labels) || data.labels.length === 0) {
            console.error('❌ Chart data has empty or invalid labels array');
            return;
        }
        
        try {
            // First, ensure any previous chart is properly destroyed
            this.destroyChart();
            
            // Get the context
            const ctx = this.chartRef.current.getContext('2d');
            if (!ctx) {
                console.error('❌ Could not get 2d context from canvas');
                return;
            }
            
            // Ensure Chart.js is loaded globally
            const ChartJS = window.Chart;
            if (!ChartJS) {
                console.error('❌ Chart.js not found globally');
                return;
            }
            
            // Create a deep copy of the data to avoid modifying props
            const chartData = {
                labels: [...data.labels],
                datasets: data.datasets.map(ds => ({...ds}))
            };
            
            // Validate first dataset has data
            const firstDataset = chartData.datasets[0];
            if (!firstDataset.data) {
                console.error('❌ First dataset has no data property');
                return;
            }
            
            if (!Array.isArray(firstDataset.data) || firstDataset.data.length === 0) {
                console.error('❌ First dataset has empty or invalid data array');
                return;
            }
            
            // Normalize all data for all chart types
            // This ensures null/undefined/NaN values don't break the chart
            firstDataset.data = firstDataset.data.map(val => {
                if (val === null || val === undefined || isNaN(Number(val))) {
                    console.warn(`⚠️ Converting invalid value to 0:`, val);
                    return 0;
                }
                return Number(val);
            });
            
            // Special handling for pie/doughnut charts
            if (type === 'pie' || type === 'doughnut') {
                // Skip rendering if all values are zero
                const allZeros = firstDataset.data.every(val => val === 0);
                if (allZeros) {
                    console.warn('⚠️ All values are zero in pie/doughnut chart');
                    firstDataset.data = [1]; // Show a single empty chart
                    chartData.labels = ['No Data'];
                    firstDataset.backgroundColor = ['#e0e0e0'];
                }
                
                // Ensure backgroundColor is an array with matching length
                if (!firstDataset.backgroundColor || !Array.isArray(firstDataset.backgroundColor)) {
                    console.log("⚠️ Adding default colors to dataset");
                    firstDataset.backgroundColor = [
                        '#3498db', '#e74c3c', '#2ecc71', '#f1c40f', '#9b59b6', 
                        '#1abc9c', '#e67e22', '#34495e', '#7f8c8d', '#d35400'
                    ];
                }
                
                // If we have more data points than colors, extend the color array
                if (firstDataset.data.length > firstDataset.backgroundColor.length) {
                    const defaultColors = [
                        '#3498db', '#e74c3c', '#2ecc71', '#f1c40f', '#9b59b6', 
                        '#1abc9c', '#e67e22', '#34495e', '#7f8c8d', '#d35400'
                    ];
                    
                    while (firstDataset.backgroundColor.length < firstDataset.data.length) {
                        const nextColor = defaultColors[firstDataset.backgroundColor.length % defaultColors.length];
                        firstDataset.backgroundColor.push(nextColor);
                    }
                }
                
                // Make sure we have matching lengths for data, labels, and colors
                if (firstDataset.data.length !== chartData.labels.length) {
                    console.warn('⚠️ Data and labels length mismatch, adjusting...');
                    const minLength = Math.min(firstDataset.data.length, chartData.labels.length);
                    firstDataset.data = firstDataset.data.slice(0, minLength);
                    chartData.labels = chartData.labels.slice(0, minLength);
                    firstDataset.backgroundColor = firstDataset.backgroundColor.slice(0, minLength);
                }
            }
            
            // For line charts, ensure we have borderColor
            if (type === 'line' && !firstDataset.borderColor) {
                firstDataset.borderColor = '#3498db';
            }
            
            console.log(`⚙️ Creating ${type || 'bar'} chart with:`, { 
                labels: chartData.labels, 
                dataPoints: firstDataset.data.length,
                hasBackgroundColor: !!firstDataset.backgroundColor,
                hasBorderColor: !!firstDataset.borderColor
            });
            
            // Create chart with error handling
            this.chartInstance = new ChartJS(ctx, {
                type: type || 'bar',
                data: chartData,
                options: this.getChartOptions()
            });
            
            console.log(`✅ Created ${type || 'bar'} chart successfully`);
        } catch (error) {
            console.error('❌ Error creating chart:', error);
            // Ensure the chart instance is nullified if creation fails
            this.chartInstance = null;
        }
    }
    
    render() {
        const { data, height, type } = this.props;
        
        // Handle empty or invalid data
        if (!data || !data.datasets || !Array.isArray(data.datasets) || data.datasets.length === 0 ||
            !data.labels || !Array.isArray(data.labels) || data.labels.length === 0) {
            
            // Show a styled empty state
            return (
                <div 
                    className="bg-white rounded-lg shadow-md p-6 flex flex-col items-center justify-center"
                    style={{ height: height || '300px' }}
                >
                    <div className="text-center text-gray-500">
                        {type === 'pie' || type === 'doughnut' ? (
                            <div className="w-24 h-24 mx-auto mb-4 rounded-full bg-gray-100 flex items-center justify-center">
                                <i className="fas fa-chart-pie text-gray-300 text-3xl"></i>
                            </div>
                        ) : type === 'line' ? (
                            <div className="w-32 h-24 mx-auto mb-4 flex items-end justify-between">
                                <div className="w-1 h-8 bg-gray-200 rounded"></div>
                                <div className="w-1 h-12 bg-gray-200 rounded"></div>
                                <div className="w-1 h-6 bg-gray-200 rounded"></div>
                                <div className="w-1 h-16 bg-gray-200 rounded"></div>
                                <div className="w-1 h-10 bg-gray-200 rounded"></div>
                            </div>
                        ) : (
                            <div className="w-32 h-24 mx-auto mb-4 flex items-end justify-between">
                                <div className="w-4 h-8 bg-gray-200 rounded-t"></div>
                                <div className="w-4 h-12 bg-gray-200 rounded-t"></div>
                                <div className="w-4 h-6 bg-gray-200 rounded-t"></div>
                                <div className="w-4 h-16 bg-gray-200 rounded-t"></div>
                                <div className="w-4 h-10 bg-gray-200 rounded-t"></div>
                            </div>
                        )}
                        
                        <p className="text-gray-500 mb-1">No data available</p>
                        <p className="text-xs text-gray-400">Data is currently being loaded or processed</p>
                    </div>
                </div>
            );
        }
        
        // Check if we have valid data in the first dataset
        if (!data.datasets[0].data || !Array.isArray(data.datasets[0].data) || data.datasets[0].data.length === 0) {
            return (
                <div 
                    className="bg-white rounded-lg shadow-md p-6 flex flex-col items-center justify-center"
                    style={{ height: height || '300px' }}
                >
                    <div className="text-center text-gray-500">
                        <div className="w-16 h-16 mx-auto mb-3 rounded-full bg-amber-50 flex items-center justify-center">
                            <i className="fas fa-exclamation-triangle text-amber-400 text-xl"></i>
                        </div>
                        <p className="text-gray-600 font-medium">Chart Data Issue</p>
                        <p className="text-xs text-gray-400 mt-1 max-w-xs">
                            The chart data structure is valid, but contains no data points to display
                        </p>
                    </div>
                </div>
            );
        }
        
        // Normal chart render
        return (
            <div className="bg-white rounded-lg shadow-md p-6">
                <div style={{ height: height || '300px' }}>
                    <canvas ref={this.chartRef}></canvas>
                </div>
            </div>
        );
    }
}

// Re-export ChartComponent as the main component
const ChartComponent = ChartComponentClass;

// For backward compatibility
const Chart = ChartComponent;

// Specialized chart components
class PieChart extends React.Component {
    render() {
        const { data, options, height } = this.props;
        return <ChartComponent type="pie" data={data} options={options} height={height} />;
    }
}

class DoughnutChart extends React.Component {
    render() {
        const { data, options, height } = this.props;
        return <ChartComponent type="doughnut" data={data} options={options} height={height} />;
    }
}

class BarChart extends React.Component {
    render() {
        const { data, options, height } = this.props;
        return <ChartComponent type="bar" data={data} options={options} height={height} />;
    }
}

class LineChart extends React.Component {
    render() {
        const { data, options, height } = this.props;
        return <ChartComponent type="line" data={data} options={options} height={height} />;
    }
}

// Make chart components available globally
window.Charts = {
    Chart,
    ChartComponent,
    PieChart,
    DoughnutChart,
    BarChart,
    LineChart
};

// For backward compatibility, also expose directly
window.ChartComponent = ChartComponent;
window.PieChart = PieChart;
window.DoughnutChart = DoughnutChart;
window.BarChart = BarChart;
window.LineChart = LineChart;
