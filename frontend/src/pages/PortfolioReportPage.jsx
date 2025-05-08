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
    const [displayFormat, setDisplayFormat] = React.useState('percent');
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
            // Make sure we're sending the correct format to the API
            console.log('Requesting report with format:', displayFormat);
            
            // Use window.api instead of direct axios call
            const report = await window.api.getPortfolioReport(
                reportDate,
                selectedLevel,
                selectedLevelKey,
                displayFormat
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
    
    // Handle display format change
    const handleDisplayFormatChange = (e) => {
        setDisplayFormat(e.target.value);
        if (reportData) {
            // Regenerate report when format changes if we already have data
            setTimeout(generateReport, 0); 
        }
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
            
            // Create workbook
            const wb = XLSX.utils.book_new();
            
            // ------------------------------------------------------
            // CONSOLIDATED VIEW - ALL SUBCATEGORY DETAILS ON ONE TAB
            // ------------------------------------------------------
            const consolidatedData = [
                ['ANTINORI FINANCIAL PORTFOLIO REPORT - CONSOLIDATED VIEW'],
                [''],
                [`Portfolio: ${reportData.portfolio || ''}`],
                [`Report Date: ${reportData.report_date || ''}`],
                [`Total Value: $${reportData.total_adjusted_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['ASSET ALLOCATION BREAKDOWN'],
                [''],
                ['Category', 'Value (%)', 'Amount ($)']
            ];
            
            // Add main asset classes first
            consolidatedData.push(
                ['EQUITY', 
                    `${reportData.equities.total_pct.toFixed(2)}%`, 
                    `$${(reportData.equities.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add equity subcategories
            Object.entries(reportData.equities.subcategories).forEach(([key, value]) => {
                if (value > 0) {
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    consolidatedData.push([`  - ${formattedKey}`, `${value.toFixed(2)}%`, `$${dollarValue}`]);
                }
            });
            
            // Add Fixed Income main category
            consolidatedData.push(
                [''],
                ['FIXED INCOME', 
                    `${reportData.fixed_income.total_pct.toFixed(2)}%`, 
                    `$${(reportData.fixed_income.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add fixed income subcategories
            Object.entries(reportData.fixed_income.subcategories).forEach(([key, value]) => {
                if (typeof value === 'object') {
                    if (value.total_pct > 0) {
                        const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                        const dollarValue = (value.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                        consolidatedData.push([`  - ${formattedKey}`, `${value.total_pct.toFixed(2)}%`, `$${dollarValue}`]);
                        
                        // Add duration breakdowns if they exist
                        if ('long_duration' in value && value.long_duration > 0) {
                            const ldValue = (value.long_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                            consolidatedData.push([`    • Long Duration`, `${value.long_duration.toFixed(2)}%`, `$${ldValue}`]);
                        }
                        if ('market_duration' in value && value.market_duration > 0) {
                            const mdValue = (value.market_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                            consolidatedData.push([`    • Market Duration`, `${value.market_duration.toFixed(2)}%`, `$${mdValue}`]);
                        }
                        if ('short_duration' in value && value.short_duration > 0) {
                            const sdValue = (value.short_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                            consolidatedData.push([`    • Short Duration`, `${value.short_duration.toFixed(2)}%`, `$${sdValue}`]);
                        }
                    }
                } else if (value > 0) {
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    consolidatedData.push([`  - ${formattedKey}`, `${value.toFixed(2)}%`, `$${dollarValue}`]);
                }
            });
            
            // Add Hard Currency category
            consolidatedData.push(
                [''],
                ['HARD CURRENCY', 
                    `${reportData.hard_currency.total_pct.toFixed(2)}%`, 
                    `$${(reportData.hard_currency.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add hard currency subcategories
            Object.entries(reportData.hard_currency.subcategories).forEach(([key, value]) => {
                if (value > 0) {
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    consolidatedData.push([`  - ${formattedKey}`, `${value.toFixed(2)}%`, `$${dollarValue}`]);
                }
            });
            
            // Add Uncorrelated Alternatives category
            consolidatedData.push(
                [''],
                ['UNCORRELATED ALTERNATIVES', 
                    `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`, 
                    `$${(reportData.uncorrelated_alternatives.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add uncorrelated alternatives subcategories
            Object.entries(reportData.uncorrelated_alternatives.subcategories).forEach(([key, value]) => {
                if (value > 0) {
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    consolidatedData.push([`  - ${formattedKey}`, `${value.toFixed(2)}%`, `$${dollarValue}`]);
                }
            });
            
            // Add Cash category
            consolidatedData.push(
                [''],
                ['CASH & CASH EQUIVALENTS', 
                    `${reportData.cash.total_pct.toFixed(2)}%`, 
                    `$${(reportData.cash.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add liquidity and performance sections
            consolidatedData.push(
                [''],
                ['LIQUIDITY'],
                [''],
                ['Category', 'Value (%)', 'Amount ($)'],
                ['Liquid Assets', 
                    `${reportData.liquidity.liquid_assets.toFixed(2)}%`, 
                    `$${(reportData.liquidity.liquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Illiquid Assets', 
                    `${reportData.liquidity.illiquid_assets.toFixed(2)}%`, 
                    `$${(reportData.liquidity.illiquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                [''],
                ['PERFORMANCE'],
                [''],
                ['Period', 'Value (%)'],
                ['1 Day', `${reportData.performance['1D'].toFixed(2)}%`],
                ['Month-to-Date (MTD)', `${reportData.performance.MTD.toFixed(2)}%`],
                ['Quarter-to-Date (QTD)', `${reportData.performance.QTD.toFixed(2)}%`],
                ['Year-to-Date (YTD)', `${reportData.performance.YTD.toFixed(2)}%`]
            );
            
            // Create consolidated worksheet
            const consolidatedWs = XLSX.utils.aoa_to_sheet(consolidatedData);
            
            // Set column widths for consolidated sheet
            const consolidatedCols = [];
            consolidatedCols[0] = { wch: 40 }; // Category/title column wider to accommodate indentation
            consolidatedCols[1] = { wch: 15 }; // Value (%) column
            consolidatedCols[2] = { wch: 20 }; // Amount ($) column
            consolidatedWs['!cols'] = consolidatedCols;
            
            // Add consolidated worksheet to workbook (as first sheet)
            XLSX.utils.book_append_sheet(wb, consolidatedWs, "All Details");
            
            // ------------------------------------------------------
            // SUMMARY WORKSHEET
            // ------------------------------------------------------
            const summaryData = [
                ['ANTINORI FINANCIAL PORTFOLIO REPORT'],
                [''],
                [`Portfolio: ${reportData.portfolio || ''}`],
                [`Report Date: ${reportData.report_date || ''}`],
                [`Total Value: $${reportData.total_adjusted_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['ASSET ALLOCATION SUMMARY'],
                ['']
            ];
            
            // Define the allocation data for the summary
            const allocationData = [
                ['Category', 'Value (%)', 'Amount ($)'],
                ['Equity', 
                    `${reportData.equities.total_pct.toFixed(2)}%`, 
                    `$${(reportData.equities.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Fixed Income', 
                    `${reportData.fixed_income.total_pct.toFixed(2)}%`, 
                    `$${(reportData.fixed_income.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Hard Currency', 
                    `${reportData.hard_currency.total_pct.toFixed(2)}%`, 
                    `$${(reportData.hard_currency.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Uncorrelated Alternatives', 
                    `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`, 
                    `$${(reportData.uncorrelated_alternatives.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Cash & Cash Equivalent', 
                    `${reportData.cash.total_pct.toFixed(2)}%`, 
                    `$${(reportData.cash.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            ];
            
            // Add allocation data to summary
            summaryData.push(...allocationData);
            
            // Add liquidity section
            summaryData.push(
                [''],
                ['LIQUIDITY'],
                [''],
                ['Category', 'Value (%)', 'Amount ($)'],
                ['Liquid Assets', 
                    `${reportData.liquidity.liquid_assets.toFixed(2)}%`, 
                    `$${(reportData.liquidity.liquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ],
                ['Illiquid Assets', 
                    `${reportData.liquidity.illiquid_assets.toFixed(2)}%`, 
                    `$${(reportData.liquidity.illiquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                ]
            );
            
            // Add performance section
            summaryData.push(
                [''],
                ['PERFORMANCE'],
                [''],
                ['Period', 'Value (%)'],
                ['1D', `${reportData.performance['1D'].toFixed(2)}%`],
                ['MTD', `${reportData.performance.MTD.toFixed(2)}%`],
                ['QTD', `${reportData.performance.QTD.toFixed(2)}%`],
                ['YTD', `${reportData.performance.YTD.toFixed(2)}%`]
            );
            
            // Create summary worksheet
            const summaryWs = XLSX.utils.aoa_to_sheet(summaryData);
            
            // Set column widths for summary sheet
            const summaryCols = [];
            summaryCols[0] = { wch: 30 }; // Category/title column
            summaryCols[1] = { wch: 15 }; // Value (%) column
            summaryCols[2] = { wch: 20 }; // Amount ($) column
            summaryWs['!cols'] = summaryCols;
            
            // Add summary worksheet to workbook
            XLSX.utils.book_append_sheet(wb, summaryWs, "Summary");
            
            // Create detailed breakdown sheets for each asset class
            
            // Equity Breakdown worksheet
            const equityData = [
                ['EQUITY BREAKDOWN'],
                [''],
                ['Total Equity Allocation', `${reportData.equities.total_pct.toFixed(2)}%`, `$${(reportData.equities.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['Subcategory', 'Value (%)', 'Amount ($)']
            ];
            
            // Add equity subcategories
            Object.entries(reportData.equities.subcategories).forEach(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                equityData.push([formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`]);
            });
            
            // Create and add equity worksheet
            const equityWs = XLSX.utils.aoa_to_sheet(equityData);
            equityWs['!cols'] = [
                { wch: 30 }, // Subcategory column
                { wch: 15 }, // Value (%) column
                { wch: 20 }  // Amount ($) column
            ];
            XLSX.utils.book_append_sheet(wb, equityWs, "Equity");
            
            // Fixed Income Breakdown worksheet
            const fiData = [
                ['FIXED INCOME BREAKDOWN'],
                [''],
                ['Total Fixed Income Allocation', `${reportData.fixed_income.total_pct.toFixed(2)}%`, `$${(reportData.fixed_income.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                ['']
            ];
            
            // Add fixed income duration info if available
            if (reportData.fixed_income.duration !== null) {
                fiData.push(['Portfolio Duration:', reportData.fixed_income.duration.toFixed(2)]);
                fiData.push(['']);
            }
            
            // Add fixed income subcategory headers
            fiData.push(['Subcategory', 'Value (%)', 'Amount ($)']);
            
            // Add fixed income subcategories
            Object.entries(reportData.fixed_income.subcategories).forEach(([key, value]) => {
                if (typeof value === 'object') {
                    // Handle nested subcategories (like government_bonds with durations)
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    fiData.push([formattedKey, `${value.total_pct.toFixed(2)}%`, `$${(value.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`]);
                    
                    // Add duration breakdowns if they exist
                    if ('long_duration' in value) {
                        fiData.push(['  - Long Duration', `${value.long_duration.toFixed(2)}%`, `$${(value.long_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`]);
                    }
                    if ('market_duration' in value) {
                        fiData.push(['  - Market Duration', `${value.market_duration.toFixed(2)}%`, `$${(value.market_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`]);
                    }
                    if ('short_duration' in value) {
                        fiData.push(['  - Short Duration', `${value.short_duration.toFixed(2)}%`, `$${(value.short_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`]);
                    }
                } else {
                    // Handle flat subcategories
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    fiData.push([formattedKey, `${value.toFixed(2)}%`, `$${(value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`]);
                }
            });
            
            // Create and add fixed income worksheet
            const fiWs = XLSX.utils.aoa_to_sheet(fiData);
            fiWs['!cols'] = [
                { wch: 30 }, // Subcategory column
                { wch: 15 }, // Value (%) column
                { wch: 20 }  // Amount ($) column
            ];
            XLSX.utils.book_append_sheet(wb, fiWs, "Fixed Income");
            
            // Hard Currency Breakdown worksheet
            const hcData = [
                ['HARD CURRENCY BREAKDOWN'],
                [''],
                ['Total Hard Currency Allocation', `${reportData.hard_currency.total_pct.toFixed(2)}%`, `$${(reportData.hard_currency.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['Subcategory', 'Value (%)', 'Amount ($)']
            ];
            
            // Add hard currency subcategories
            Object.entries(reportData.hard_currency.subcategories).forEach(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                hcData.push([formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`]);
            });
            
            // Create and add hard currency worksheet
            const hcWs = XLSX.utils.aoa_to_sheet(hcData);
            hcWs['!cols'] = [
                { wch: 30 }, // Subcategory column
                { wch: 15 }, // Value (%) column
                { wch: 20 }  // Amount ($) column
            ];
            XLSX.utils.book_append_sheet(wb, hcWs, "Hard Currency");
            
            // Uncorrelated Alternatives Breakdown worksheet
            const uaData = [
                ['UNCORRELATED ALTERNATIVES BREAKDOWN'],
                [''],
                ['Total Uncorrelated Alternatives Allocation', `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`, `$${(reportData.uncorrelated_alternatives.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
                [''],
                ['Subcategory', 'Value (%)', 'Amount ($)']
            ];
            
            // Add uncorrelated alternatives subcategories
            Object.entries(reportData.uncorrelated_alternatives.subcategories).forEach(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                uaData.push([formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`]);
            });
            
            // Create and add uncorrelated alternatives worksheet
            const uaWs = XLSX.utils.aoa_to_sheet(uaData);
            uaWs['!cols'] = [
                { wch: 30 }, // Subcategory column
                { wch: 15 }, // Value (%) column
                { wch: 20 }  // Amount ($) column
            ];
            XLSX.utils.book_append_sheet(wb, uaWs, "Uncorrelated Alternatives");
            
            // Add a Performance worksheet
            const perfData = [
                ['PERFORMANCE REPORT'],
                [''],
                ['Portfolio', reportData.portfolio || ''],
                ['Report Date', reportData.report_date || ''],
                [''],
                ['Period', 'Return (%)'],
                ['1 Day', `${reportData.performance['1D'].toFixed(2)}%`],
                ['Month-to-Date (MTD)', `${reportData.performance.MTD.toFixed(2)}%`],
                ['Quarter-to-Date (QTD)', `${reportData.performance.QTD.toFixed(2)}%`],
                ['Year-to-Date (YTD)', `${reportData.performance.YTD.toFixed(2)}%`]
            ];
            
            // Create and add performance worksheet
            const perfWs = XLSX.utils.aoa_to_sheet(perfData);
            perfWs['!cols'] = [
                { wch: 30 }, // Period column
                { wch: 15 }   // Return (%) column
            ];
            XLSX.utils.book_append_sheet(wb, perfWs, "Performance");
            
            // Generate Excel file with a professional name
            XLSX.writeFile(wb, `ANTINORI_Portfolio_Report_${reportData.portfolio || 'Portfolio'}_${reportData.report_date.replace(/\//g, '-')}.xlsx`);
            
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
            const { jsPDF } = window.jspdf;
            // Access autotable plugin as a method on the jsPDF instance
            // This is the proper way to access the plugin since it's added to the jsPDF prototype
            
            // Create new PDF document
            const doc = new jsPDF();
            
            // Add title with branding
            doc.setFontSize(20);
            doc.setTextColor(20, 83, 45); // Dark green to match brand
            doc.text('ANTINORI FINANCIAL', 14, 20);
            
            doc.setFontSize(18);
            doc.setTextColor(15, 23, 42); // Dark slate blue for subtitles
            doc.text('PORTFOLIO REPORT', 14, 28);
            
            // Add metadata
            doc.setFontSize(12);
            doc.setTextColor(60, 60, 60); // Dark gray for normal text
            doc.text(`Portfolio: ${reportData.portfolio || ''}`, 14, 38);
            doc.text(`Report Date: ${reportData.report_date || ''}`, 14, 44);
            doc.text(`Total Value: $${reportData.total_adjusted_value.toLocaleString('en-US', 
                { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, 14, 50);
            
            // Asset Allocation Table with dollar amounts
            doc.autoTable({
                startY: 56,
                head: [['Asset Allocation', 'Value (%)', 'Amount ($)']],
                body: [
                    [
                        'Equity', 
                        `${reportData.equities.total_pct.toFixed(2)}%`,
                        `$${(reportData.equities.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ],
                    [
                        'Fixed Income', 
                        `${reportData.fixed_income.total_pct.toFixed(2)}%`,
                        `$${(reportData.fixed_income.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ],
                    [
                        'Hard Currency', 
                        `${reportData.hard_currency.total_pct.toFixed(2)}%`,
                        `$${(reportData.hard_currency.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ],
                    [
                        'Uncorrelated Alternatives', 
                        `${reportData.uncorrelated_alternatives.total_pct.toFixed(2)}%`,
                        `$${(reportData.uncorrelated_alternatives.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ],
                    [
                        'Cash & Cash Equivalent', 
                        `${reportData.cash.total_pct.toFixed(2)}%`,
                        `$${(reportData.cash.total_pct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ]
                ],
                theme: 'striped',
                headStyles: { fillColor: [20, 83, 45] }, // Dark green
                styles: { fontSize: 10 }
            });
            
            // Equity Breakdown with dollar amounts
            const equityData = Object.entries(reportData.equities.subcategories).map(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                return [formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`];
            });
            
            doc.autoTable({
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Equity Breakdown', 'Value (%)', 'Amount ($)']],
                body: equityData,
                theme: 'striped',
                headStyles: { fillColor: [100, 149, 237] }, // Light blue
                styles: { fontSize: 10 }
            });
            
            // Fixed Income Breakdown with dollar amounts
            const fiData = [];
            
            // Process fixed income subcategories
            Object.entries(reportData.fixed_income.subcategories).forEach(([key, value]) => {
                if (typeof value === 'object') {
                    // Handle nested subcategories (like government_bonds with durations)
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const totalPct = value.total_pct || 0;
                    const dollarValue = (totalPct * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    fiData.push([formattedKey, `${totalPct.toFixed(2)}%`, `$${dollarValue}`]);
                    
                    // Add duration breakdowns if they exist
                    if ('long_duration' in value && value.long_duration > 0) {
                        const ldValue = (value.long_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                        fiData.push(['  - Long Duration', `${value.long_duration.toFixed(2)}%`, `$${ldValue}`]);
                    }
                    if ('market_duration' in value && value.market_duration > 0) {
                        const mdValue = (value.market_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                        fiData.push(['  - Market Duration', `${value.market_duration.toFixed(2)}%`, `$${mdValue}`]);
                    }
                    if ('short_duration' in value && value.short_duration > 0) {
                        const sdValue = (value.short_duration * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                        fiData.push(['  - Short Duration', `${value.short_duration.toFixed(2)}%`, `$${sdValue}`]);
                    }
                } else {
                    // Handle flat subcategories
                    const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    fiData.push([formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`]);
                }
            });
            
            if (fiData.length > 0) {
                doc.autoTable({
                    startY: doc.lastAutoTable.finalY + 10,
                    head: [['Fixed Income Breakdown', 'Value (%)', 'Amount ($)']],
                    body: fiData,
                    theme: 'striped',
                    headStyles: { fillColor: [255, 102, 102] }, // Light red
                    styles: { fontSize: 10 }
                });
            }
            
            // Hard Currency Breakdown with dollar amounts
            const hcData = Object.entries(reportData.hard_currency.subcategories).filter(([_key, value]) => value > 0).map(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                return [formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`];
            });
            
            if (hcData.length > 0) {
                doc.autoTable({
                    startY: doc.lastAutoTable.finalY + 10,
                    head: [['Hard Currency Breakdown', 'Value (%)', 'Amount ($)']],
                    body: hcData,
                    theme: 'striped',
                    headStyles: { fillColor: [255, 217, 102] }, // Light yellow
                    styles: { fontSize: 10 }
                });
            }
            
            // Uncorrelated Alternatives Breakdown with dollar amounts
            const uaData = Object.entries(reportData.uncorrelated_alternatives.subcategories).filter(([_key, value]) => value > 0).map(([key, value]) => {
                const formattedKey = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const dollarValue = (value * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                return [formattedKey, `${value.toFixed(2)}%`, `$${dollarValue}`];
            });
            
            if (uaData.length > 0) {
                doc.autoTable({
                    startY: doc.lastAutoTable.finalY + 10,
                    head: [['Uncorrelated Alternatives Breakdown', 'Value (%)', 'Amount ($)']],
                    body: uaData,
                    theme: 'striped',
                    headStyles: { fillColor: [255, 153, 51] }, // Light orange
                    styles: { fontSize: 10 }
                });
            }
            
            // Liquidity Table with dollar amounts
            doc.autoTable({
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Liquidity', 'Value (%)', 'Amount ($)']],
                body: [
                    [
                        'Liquid Assets', 
                        `${reportData.liquidity.liquid_assets.toFixed(2)}%`,
                        `$${(reportData.liquidity.liquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ],
                    [
                        'Illiquid Assets', 
                        `${reportData.liquidity.illiquid_assets.toFixed(2)}%`,
                        `$${(reportData.liquidity.illiquid_assets * reportData.total_adjusted_value / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                    ]
                ],
                theme: 'striped',
                headStyles: { fillColor: [169, 169, 169] }, // Light gray
                styles: { fontSize: 10 }
            });
            
            // Performance Table - keep this as percentage only
            doc.autoTable({
                startY: doc.lastAutoTable.finalY + 10,
                head: [['Performance', 'Value (%)']],
                body: [
                    ['1 Day', `${reportData.performance['1D'].toFixed(2)}%`],
                    ['Month to Date (MTD)', `${reportData.performance.MTD.toFixed(2)}%`],
                    ['Quarter to Date (QTD)', `${reportData.performance.QTD.toFixed(2)}%`],
                    ['Year to Date (YTD)', `${reportData.performance.YTD.toFixed(2)}%`]
                ],
                theme: 'striped',
                headStyles: { fillColor: [147, 112, 219] }, // Light purple
                styles: { fontSize: 10 }
            });
            
            // Footer with timestamp
            const timestamp = new Date().toLocaleString('en-US');
            doc.setFontSize(8);
            doc.setTextColor(100, 100, 100); // Light gray
            doc.text(`Report generated on ${timestamp} | ANTINORI Financial Portfolio Management System`, 14, doc.internal.pageSize.height - 10);
            
            // Save the PDF with professional naming
            doc.save(`ANTINORI_Portfolio_Report_${reportData.portfolio || 'Portfolio'}_${reportData.report_date.replace(/\//g, '-')}.pdf`);
            
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
                        className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 focus:outline-none flex items-center"
                        onClick={() => {
                            const newFormat = displayFormat === 'percent' ? 'dollar' : 'percent';
                            console.log('Toggle button clicked: changing format from', displayFormat, 'to', newFormat);
                            setDisplayFormat(newFormat);
                            if (reportData) {
                                // Regenerate report when format changes if we already have data
                                setTimeout(generateReport, 0);
                            }
                        }}
                        disabled={!reportData}
                    >
                        <i className={displayFormat === 'percent' ? 'fas fa-dollar-sign mr-2' : 'fas fa-percentage mr-2'}></i>
                        {displayFormat === 'percent' ? 'Show Dollar Values' : 'Show Percentages'}
                    </button>
                    
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
                <PortfolioReport 
                    reportData={reportData} 
                    loading={loading} 
                    displayFormat={displayFormat} 
                />
            )}
        </div>
    );
};

// No export needed with window assignment