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
        if (this.chartInstance) {
            this.chartInstance.destroy();
            this.chartInstance = null;
        }
    }
    
    createChart() {
        const { data, type } = this.props;
        
        if (!this.chartRef.current || !data) {
            return;
        }
        
        try {
            const ctx = this.chartRef.current.getContext('2d');
            const ChartJS = window.Chart; // Use the global Chart object
            
            this.chartInstance = new ChartJS(ctx, {
                type: type || 'bar',
                data: data,
                options: this.getChartOptions()
            });
        } catch (error) {
            console.error('Error creating chart:', error);
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
