// Professional Portfolio Report Page
// No import needed here since we're using script tags in index.html
// and all components are declared globally in the browser environment

window.PortfolioReportPage = () => {
    const [loading, setLoading] = React.useState(false);
    const [reportData, setReportData] = React.useState(null);
    const [error, setError] = React.useState(null);
    const [selectedLevel, setSelectedLevel] = React.useState('portfolio');
    const [selectedLevelKey, setSelectedLevelKey] = React.useState('');
    const [levelOptions, setLevelOptions] = React.useState([]);
    const [reportDate, setReportDate] = React.useState('2025-05-01');
    const [exportLoading, setExportLoading] = React.useState({ excel: false, pdf: false });
    
    // Fetch entity options when level changes
    React.useEffect(() => {
        const fetchOptions = async () => {
            try {
                console.log('Fetching options for level:', selectedLevel);
                // Use window.api instead of direct axios call
                const options = await window.api.getEntityOptions(selectedLevel);
                
                console.log('Successfully loaded options:', options);
                setLevelOptions(options);
                
                // Set default selection if options are available and nothing is selected
                if (options.length > 0 && !selectedLevelKey) {
                    setSelectedLevelKey(options[0].key);
                }
            } catch (err) {
                console.error('Entity options error:', err);
                setError('Failed to load entity options');
            }
        };
        
        fetchOptions();
    }, [selectedLevel]);
    
    // Generate report when level, level key or date changes
    const generateReport = async () => {
        if (!selectedLevelKey) return;
        
        setLoading(true);
        setError(null);
        
        try {
            // Use window.api instead of direct axios call
            const report = await window.api.getPortfolioReport(
                reportDate,
                selectedLevel,
                selectedLevelKey
            );
            
            console.log('Portfolio report data:', report);
            
            if (report) {
                setReportData(report);
            } else {
                setError('No data received from server');
            }
        } catch (err) {
            console.error('Error generating report:', err);
            setError('Failed to generate portfolio report');
        } finally {
            setLoading(false);
        }
    };
    
    // Handle level change
    const handleLevelChange = (e) => {
        setSelectedLevel(e.target.value);
        setSelectedLevelKey(''); // Reset level key when level changes
    };
    
    // Handle level key change
    const handleLevelKeyChange = (e) => {
        setSelectedLevelKey(e.target.value);
    };
    
    // Handle date change
    const handleDateChange = (e) => {
        setReportDate(e.target.value);
    };
    
    // Handle form submission
    const handleSubmit = (e) => {
        e.preventDefault();
        generateReport();
    };
    
    // Export report as Excel
    const exportToExcel = async () => {
        if (!reportData) {
            alert('Please generate a report first');
            return;
        }
        
        try {
            setExportLoading(prev => ({ ...prev, excel: true }));
            
            // Use XLSX library to create an Excel file
            const XLSX = window.XLSX;
            
            // Create workbook and add worksheets
            const wb = XLSX.utils.book_new();
            
            // Format all data into a proper structure for Excel
            const data = [
                ['Portfolio Report'],
                [`Portfolio: ${reportData.portfolio || ''}`],
                [`Date: ${reportData.report_date || ''}`],
                [`Total Value: $${reportData.total_adjusted_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['Asset Allocation'],
                ['Category', 'Value (%)'],
                ['Equity', `${reportData.equities.total_pct.toFixed(2)}%`],
                ['Fixed Income', `${reportData.fixed_income.total_pct.toFixed(2)}%`],
                ['Hard Currency', `${reportData.hard_currency.total_pct.toFixed(2)}%`],
                ['Uncorrelated Alternatives', `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`],
                ['Cash & Cash Equivalent', `${reportData.cash.total_pct.toFixed(2)}%`],
                [''],
                ['Liquidity'],
                ['Category', 'Value (%)'],
                ['Liquid Assets', `${reportData.liquidity.liquid_assets.toFixed(2)}%`],
                ['Illiquid Assets', `${reportData.liquidity.illiquid_assets.toFixed(2)}%`],
                [''],
                ['Performance'],
                ['Period', 'Value (%)'],
                ['1D', `${reportData.performance['1D'].toFixed(2)}%`],
                ['MTD', `${reportData.performance.MTD.toFixed(2)}%`],
                ['QTD', `${reportData.performance.QTD.toFixed(2)}%`],
                ['YTD', `${reportData.performance.YTD.toFixed(2)}%`]
            ];
            
            // Add all subcategories from each asset class
            // For equity subcategories
            data.push([''], ['Equity Breakdown'], ['Subcategory', 'Value (%)']);
            Object.entries(reportData.equities.subcategories).forEach(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                data.push([formattedKey, `${value.toFixed(2)}%`]);
            });
            
            // Create worksheet and add to workbook
            const ws = XLSX.utils.aoa_to_sheet(data);
            XLSX.utils.book_append_sheet(wb, ws, "Portfolio Report");
            
            // Style cells (limited in XLSX)
            // By setting column widths
            const cols = ws['!cols'] || [];
            cols[0] = { wch: 30 }; // Set column width for column A
            cols[1] = { wch: 15 }; // Set column width for column B
            ws['!cols'] = cols;
            
            // Generate Excel file and download
            XLSX.writeFile(wb, `${reportData.portfolio || 'Portfolio'}_Report_${reportData.report_date.replace(/\//g, '-')}.xlsx`);
            
        } catch (error) {
            console.error('Excel export error:', error);
            alert('Failed to export to Excel: ' + error.message);
        } finally {
            setExportLoading(prev => ({ ...prev, excel: false }));
        }
    };
    
    // Export report as PDF
    const exportToPDF = async () => {
        if (!reportData) {
            alert('Please generate a report first');
            return;
        }
        
        try {
            setExportLoading(prev => ({ ...prev, pdf: true }));
            
            // Use jsPDF library
            const jsPDF = window.jspdf.jsPDF;
            const autoTable = window.jspdf.autoTable;
            
            // Create new PDF document
            const doc = new jsPDF();
            
            // Add title
            doc.setFontSize(18);
            doc.text('Portfolio Report', 14, 22);
            
            // Add metadata
            doc.setFontSize(12);
            doc.text(`Portfolio: ${reportData.portfolio || ''}`, 14, 32);
            doc.text(`Date: ${reportData.report_date || ''}`, 14, 38);
            doc.text(`Total Value: $${reportData.total_adjusted_value.toLocaleString('en-US', 
                { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, 14, 44);
            
            // Asset Allocation Table
            autoTable(doc, {
                startY: 50,
                head: [['Asset Allocation', 'Value (%)']],
                body: [
                    ['Equity', `${reportData.equities.total_pct.toFixed(2)}%`],
                    ['Fixed Income', `${reportData.fixed_income.total_pct.toFixed(2)}%`],
                    ['Hard Currency', `${reportData.hard_currency.total_pct.toFixed(2)}%`],
                    ['Uncorrelated Alternatives', `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`],
                    ['Cash & Cash Equivalent', `${reportData.cash.total_pct.toFixed(2)}%`]
                ],
                theme: 'striped',
                headStyles: { fillColor: [20, 83, 45] }, // Dark green
                styles: { fontSize: 10 }
            });
            
            // Equity Breakdown
            const equityData = Object.entries(reportData.equities.subcategories).map(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                return [formattedKey, `${value.toFixed(2)}%`];
            });
            
            autoTable(doc, {
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Equity Breakdown', 'Value (%)']],
                body: equityData,
                theme: 'striped',
                headStyles: { fillColor: [100, 149, 237] }, // Light blue
                styles: { fontSize: 10 }
            });
            
            // Liquidity Table
            autoTable(doc, {
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Liquidity', 'Value (%)']],
                body: [
                    ['Liquid Assets', `${reportData.liquidity.liquid_assets.toFixed(2)}%`],
                    ['Illiquid Assets', `${reportData.liquidity.illiquid_assets.toFixed(2)}%`]
                ],
                theme: 'striped',
                headStyles: { fillColor: [169, 169, 169] }, // Light gray
                styles: { fontSize: 10 }
            });
            
            // Performance Table
            autoTable(doc, {
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Performance', 'Value (%)']],
                body: [
                    ['1D', `${reportData.performance['1D'].toFixed(2)}%`],
                    ['MTD', `${reportData.performance.MTD.toFixed(2)}%`],
                    ['QTD', `${reportData.performance.QTD.toFixed(2)}%`],
                    ['YTD', `${reportData.performance.YTD.toFixed(2)}%`]
                ],
                theme: 'striped',
                headStyles: { fillColor: [147, 112, 219] }, // Light purple
                styles: { fontSize: 10 }
            });
            
            // Save the PDF
            doc.save(`${reportData.portfolio || 'Portfolio'}_Report_${reportData.report_date.replace(/\//g, '-')}.pdf`);
            
        } catch (error) {
            console.error('PDF export error:', error);
            alert('Failed to export to PDF: ' + error.message);
        } finally {
            setExportLoading(prev => ({ ...prev, pdf: false }));
        }
    };
    
    // Format date for display
    const formatDate = (dateStr) => {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', {
            month: 'long',
            day: 'numeric',
            year: 'numeric'
        });
    };
    
    return (
        <div className="container mx-auto px-4 py-8">
            {/* Page header */}
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h1 className="text-2xl font-bold text-gray-800">Portfolio Report</h1>
                    <p className="text-gray-600">Generate and view detailed portfolio reports</p>
                </div>
                
                <div className="flex space-x-2">
                    <button
                        className="px-4 py-2 bg-green-800 text-white rounded hover:bg-green-700 focus:outline-none flex items-center"
                        onClick={exportToExcel}
                        disabled={exportLoading.excel || !reportData}
                    >
                        {exportLoading.excel ? (
                            <i className="fas fa-circle-notch fa-spin mr-2"></i>
                        ) : (
                            <i className="fas fa-file-excel mr-2"></i>
                        )}
                        Export to Excel
                    </button>
                    
                    <button
                        className="px-4 py-2 bg-red-800 text-white rounded hover:bg-red-700 focus:outline-none flex items-center"
                        onClick={exportToPDF}
                        disabled={exportLoading.pdf || !reportData}
                    >
                        {exportLoading.pdf ? (
                            <i className="fas fa-circle-notch fa-spin mr-2"></i>
                        ) : (
                            <i className="fas fa-file-pdf mr-2"></i>
                        )}
                        Export to PDF
                    </button>
                </div>
            </div>
            
            {/* Report options */}
            <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                <form onSubmit={handleSubmit} className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Level
                        </label>
                        <select
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={selectedLevel}
                            onChange={handleLevelChange}
                        >
                            <option value="client">Client</option>
                            <option value="portfolio">Portfolio</option>
                            <option value="account">Account</option>
                        </select>
                    </div>
                    
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            {selectedLevel === 'client' ? 'Client' : 
                             selectedLevel === 'portfolio' ? 'Portfolio' : 'Account'}
                        </label>
                        <select
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={selectedLevelKey}
                            onChange={handleLevelKeyChange}
                        >
                            <option value="">Select...</option>
                            {levelOptions.map(option => (
                                <option key={option.key} value={option.key}>
                                    {option.display}
                                </option>
                            ))}
                        </select>
                    </div>
                    
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Report Date
                        </label>
                        <input
                            type="date"
                            className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-green-500 focus:border-green-500"
                            value={reportDate}
                            onChange={handleDateChange}
                        />
                    </div>
                    
                    <div className="flex items-end">
                        <button
                            type="submit"
                            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none w-full"
                            disabled={!selectedLevelKey || loading}
                        >
                            {loading ? (
                                <span className="flex items-center justify-center">
                                    <i className="fas fa-circle-notch fa-spin mr-2"></i>
                                    Loading...
                                </span>
                            ) : 'Generate Report'}
                        </button>
                    </div>
                </form>
            </div>
            
            {/* Error display */}
            {error && (
                <div className="bg-red-50 border-l-4 border-red-500 p-4 mb-6">
                    <div className="flex">
                        <div className="flex-shrink-0">
                            <i className="fas fa-exclamation-circle text-red-500"></i>
                        </div>
                        <div className="ml-3">
                            <p className="text-sm text-red-700">{error}</p>
                        </div>
                    </div>
                </div>
            )}
            
            {/* Report display */}
            {reportData && (
                <PortfolioReport reportData={reportData} loading={loading} />
            )}
        </div>
    );
};

// No export needed with window assignment