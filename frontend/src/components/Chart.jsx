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
        
        console.log(`🔍 Attempting to create ${type || 'bar'} chart with data:`, data);
        
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
        
        if (data.datasets.length === 0) {
            console.error('❌ Chart data has empty datasets array');
            return;
        }
        
        // Ensure labels exist
        if (!data.labels) {
            console.error('❌ Chart data is missing labels property');
            return;
        }
        
        if (data.labels.length === 0) {
            console.error('❌ Chart data has empty labels array');
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
            
            // Validate first dataset has data
            const firstDataset = data.datasets[0];
            if (!firstDataset.data) {
                console.error('❌ First dataset has no data property');
                return;
            }
            
            if (firstDataset.data.length === 0) {
                console.error('❌ First dataset has empty data array');
                return;
            }
            
            // Normalize the data for pie/doughnut charts
            // This ensures null/undefined values don't break the chart
            if (type === 'pie' || type === 'doughnut') {
                firstDataset.data = firstDataset.data.map(val => 
                    (val === null || val === undefined) ? 0 : val
                );
                
                // Ensure backgroundColor is an array with matching length
                if (!firstDataset.backgroundColor || !Array.isArray(firstDataset.backgroundColor)) {
                    console.log("⚠️ Adding default colors to dataset");
                    firstDataset.backgroundColor = [
                        '#3498db', '#e74c3c', '#f1c40f', '#2ecc71', '#9b59b6', 
                        '#1abc9c', '#e67e22', '#34495e', '#7f8c8d', '#d35400'
                    ];
                }
                
                // If we have more data points than colors, extend the color array
                if (firstDataset.data.length > firstDataset.backgroundColor.length) {
                    const defaultColors = [
                        '#3498db', '#e74c3c', '#f1c40f', '#2ecc71', '#9b59b6', 
                        '#1abc9c', '#e67e22', '#34495e', '#7f8c8d', '#d35400'
                    ];
                    
                    while (firstDataset.backgroundColor.length < firstDataset.data.length) {
                        const nextColor = defaultColors[firstDataset.backgroundColor.length % defaultColors.length];
                        firstDataset.backgroundColor.push(nextColor);
                    }
                }
            }
            
            console.log(`⚙️ Creating ${type || 'bar'} chart with:`, { 
                labels: data.labels, 
                dataPoints: firstDataset.data,
                backgroundColor: firstDataset.backgroundColor
            });
            
            // Create chart with error handling
            this.chartInstance = new ChartJS(ctx, {
                type: type || 'bar',
                data: data,
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
        const { data, height } = this.props;
        
        // Handle empty data
        if (!data || !data.datasets || data.datasets.length === 0) {
            return (
                <div 
                    className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center"
                    style={{ height: height || '300px' }}
                >
                    <div className="text-center text-gray-500">
                        <i className="fas fa-chart-bar text-2xl mb-2"></i>
                        <p>No chart data available.</p>
                    </div>
                </div>
            );
        }
        
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
