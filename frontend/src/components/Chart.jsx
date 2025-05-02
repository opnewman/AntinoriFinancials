// Rename our component to ChartComponent to avoid collision with Chart.js global object
const ChartComponent = ({ type, data, options, height }) => {
    const chartRef = React.useRef(null);
    const chartInstanceRef = React.useRef(null);
    
    // Default chart options
    const defaultOptions = {
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
    
    // Merge default options with provided options
    const chartOptions = {
        ...defaultOptions,
        ...(options || {})
    };
    
    // Create or update chart on data or type change
    React.useEffect(() => {
        if (!chartRef.current || !data) return;
        
        // Clean up any existing chart
        if (chartInstanceRef.current) {
            chartInstanceRef.current.destroy();
        }
        
        // Create new chart instance
        const ctx = chartRef.current.getContext('2d');
        const ChartJS = window.Chart; // Use the global Chart object
        
        chartInstanceRef.current = new ChartJS(ctx, {
            type: type || 'bar',
            data: data,
            options: chartOptions
        });
        
        // Clean up on unmount
        return () => {
            if (chartInstanceRef.current) {
                chartInstanceRef.current.destroy();
                chartInstanceRef.current = null;
            }
        };
    }, [data, type, chartOptions]);
    
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
                <canvas ref={chartRef}></canvas>
            </div>
        </div>
    );
};

// For backward compatibility
const Chart = ChartComponent;

// Specialized chart components
const PieChart = ({ data, options, height }) => {
    return <ChartComponent type="pie" data={data} options={options} height={height} />;
};

const DoughnutChart = ({ data, options, height }) => {
    return <ChartComponent type="doughnut" data={data} options={options} height={height} />;
};

const BarChart = ({ data, options, height }) => {
    return <ChartComponent type="bar" data={data} options={options} height={height} />;
};

const LineChart = ({ data, options, height }) => {
    return <ChartComponent type="line" data={data} options={options} height={height} />;
};
