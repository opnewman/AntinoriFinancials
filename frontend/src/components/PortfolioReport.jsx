// Professional portfolio report component to match exactly the Excel template format
window.PortfolioReport = ({ reportData, loading }) => {
    const formatDate = (dateStr) => {
        if (!dateStr) return '';
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', { 
            month: '2-digit', 
            day: '2-digit', 
            year: 'numeric' 
        });
    };
    
    // Format percentage for display
    const formatPercent = (value) => {
        if (value === null || value === undefined) return '';
        return parseFloat(value).toFixed(2) + '%';
    };
    
    // Format currency for display
    const formatCurrency = (value) => {
        if (value === null || value === undefined) return '';
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }).format(value);
    };
    
    // Format number with 2 decimal places
    const formatNumber = (value) => {
        if (value === null || value === undefined) return '';
        return parseFloat(value).toFixed(2);
    };
    
    // Check if a value is above model thresholds
    const isAboveModel = (value, threshold = 5) => {
        return value > threshold;
    };
    
    // Check if a value is below model thresholds
    const isBelowModel = (value, threshold = 5) => {
        return value < -threshold;
    };
    
    // Get CSS class based on model adherence
    const getModelClass = (value, threshold = 5) => {
        if (isAboveModel(value, threshold)) return 'text-red-600 font-bold';
        if (isBelowModel(value, threshold)) return 'text-yellow-600 font-bold';
        return '';
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center h-64">
                <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800"></div>
                <span className="ml-4 text-lg font-medium text-gray-600">Loading portfolio report...</span>
            </div>
        );
    }

    if (!reportData) {
        return (
            <div className="bg-white p-6 rounded-lg shadow-md text-center">
                <i className="fas fa-file-excel text-4xl text-gray-400 mb-4"></i>
                <h3 className="text-xl font-medium text-gray-700">No Report Data</h3>
                <p className="text-gray-500 mt-2">
                    Please select a portfolio and date to generate a report.
                </p>
            </div>
        );
    }

    const {
        report_date,
        portfolio,
        total_adjusted_value,
        equities,
        fixed_income,
        hard_currency,
        uncorrelated_alternatives,
        cash,
        liquidity,
        performance
    } = reportData;

    return (
        <div className="portfolio-report bg-white p-6 rounded-lg shadow-md overflow-x-auto">
            {/* Report header */}
            <div className="grid grid-cols-3 gap-4 mb-6">
                <div>
                    <span className="text-gray-600 text-sm">PM: Justin</span>
                </div>
                <div className="text-center">
                    <span className="text-gray-600 text-sm">{formatDate(report_date)}</span>
                </div>
                <div className="text-right">
                    <div className="flex justify-end items-center gap-2">
                        <div className="bg-yellow-200 px-2 py-0.5 text-xs">
                            Indicates 5%+ Off Model
                        </div>
                        <div className="bg-red-200 px-2 py-0.5 text-xs">
                            Indicates 15%+ Off Model
                        </div>
                    </div>
                </div>
            </div>

            <div className="text-lg font-semibold text-gray-800 mb-4">Model Portfolios</div>

            {/* Portfolio report table */}
            <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 text-sm">
                    <thead>
                        <tr>
                            <th className="bg-blue-800 text-white px-2 py-2 text-left w-36"></th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Model Portfolio</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Atlas, Jay & Target</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">BroadRidge Digital Set & ETF</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">BroadRidge ESG & Env.</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Ephesium Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Griffith GST Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Gabriel, Rotem Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Justin and Maria Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Kingsbury Emergent Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Revitalion Expertise</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Collins FLAT Model</th>
                            <th className="bg-blue-800 text-white px-2 py-2 text-center">Yellowstone Sam & Erin GST</th>
                        </tr>
                        <tr>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-left">Portfolio</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Balanced Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Growth Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                            <th className="bg-gray-100 text-gray-700 px-2 py-1 text-center">Model</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                        {/* EQUITIES SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Equities</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">52.00%</td>
                            <td className="px-2 py-1.5 text-center">48.36%</td>
                            <td className="px-2 py-1.5 text-center">48.00%</td>
                            <td className="px-2 py-1.5 text-center">45.00%</td>
                            <td className="px-2 py-1.5 text-center">57.75%</td>
                            <td className="px-2 py-1.5 text-center">47.00%</td>
                            <td className="px-2 py-1.5 text-center">47.48%</td>
                            <td className="px-2 py-1.5 text-center">56.67%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">49.46%</td>
                            <td className="px-2 py-1.5 text-center">56.82%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Vol</td>
                            <td className="px-2 py-1.5 text-center">{formatNumber(equities.vol) || '-'}</td>
                            <td className="px-2 py-1.5 text-center">28.00</td>
                            <td className="px-2 py-1.5 text-center">23.27</td>
                            <td className="px-2 py-1.5 text-center">28.15</td>
                            <td className="px-2 py-1.5 text-center">25.95</td>
                            <td className="px-2 py-1.5 text-center">26.71</td>
                            <td className="px-2 py-1.5 text-center">27.23</td>
                            <td className="px-2 py-1.5 text-center">22.75</td>
                            <td className="px-2 py-1.5 text-center">24.81</td>
                            <td className="px-2 py-1.5 text-center">26.58</td>
                            <td className="px-2 py-1.5 text-center">0.00</td>
                            <td className="px-2 py-1.5 text-center">17.39</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Beta</td>
                            <td className="px-2 py-1.5 text-center">{formatNumber(equities.beta) || '-'}</td>
                            <td className="px-2 py-1.5 text-center">0.85</td>
                            <td className="px-2 py-1.5 text-center">0.83</td>
                            <td className="px-2 py-1.5 text-center">0.72</td>
                            <td className="px-2 py-1.5 text-center">0.75</td>
                            <td className="px-2 py-1.5 text-center">0.76</td>
                            <td className="px-2 py-1.5 text-center">0.70</td>
                            <td className="px-2 py-1.5 text-center">0.81</td>
                            <td className="px-2 py-1.5 text-center">0.71</td>
                            <td className="px-2 py-1.5 text-center">0.00</td>
                            <td className="px-2 py-1.5 text-center">0.71</td>
                            <td className="px-2 py-1.5 text-center">0.92</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Beta Adjusted</td>
                            <td className="px-2 py-1.5 text-center">{formatNumber(equities.beta_adjusted) || '-'}</td>
                            <td className="px-2 py-1.5 text-center bg-yellow-100">20.76%</td>
                            <td className="px-2 py-1.5 text-center">24.25%</td>
                            <td className="px-2 py-1.5 text-center">22.09%</td>
                            <td className="px-2 py-1.5 text-center">22.50%</td>
                            <td className="px-2 py-1.5 text-center">24.45%</td>
                            <td className="px-2 py-1.5 text-center">20.58%</td>
                            <td className="px-2 py-1.5 text-center">23.40%</td>
                            <td className="px-2 py-1.5 text-center">29.87%</td>
                            <td className="px-2 py-1.5 text-center">21.89%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">26.31%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">US Markets</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.us_markets)}</td>
                            <td className="px-2 py-1.5 text-center">0.23%</td>
                            <td className="px-2 py-1.5 text-center">0.24%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">0.35%</td>
                            <td className="px-2 py-1.5 text-center">0.48%</td>
                            <td className="px-2 py-1.5 text-center">2.36%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Global Markets</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.global_markets)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">3.54%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">0.35%</td>
                            <td className="px-2 py-1.5 text-center">4.30%</td>
                            <td className="px-2 py-1.5 text-center">2.36%</td>
                            <td className="px-2 py-1.5 text-center">3.00%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.53%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Emerging Markets</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.emerging_markets)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">4.90%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Commodities</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.commodities)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">3.54%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.17%</td>
                            <td className="px-2 py-1.5 text-center">11.16%</td>
                            <td className="px-2 py-1.5 text-center">4.90%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.53%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Real Estate</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.real_estate)}</td>
                            <td className="px-2 py-1.5 text-center">2.71%</td>
                            <td className="px-2 py-1.5 text-center">3.35%</td>
                            <td className="px-2 py-1.5 text-center">3.07%</td>
                            <td className="px-2 py-1.5 text-center">3.44%</td>
                            <td className="px-2 py-1.5 text-center">2.64%</td>
                            <td className="px-2 py-1.5 text-center">0.35%</td>
                            <td className="px-2 py-1.5 text-center">1.29%</td>
                            <td className="px-2 py-1.5 text-center">2.42%</td>
                            <td className="px-2 py-1.5 text-center">2.01%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Private Equity</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.private_equity)}</td>
                            <td className="px-2 py-1.5 text-center">4.62%</td>
                            <td className="px-2 py-1.5 text-center">3.07%</td>
                            <td className="px-2 py-1.5 text-center">2.15%</td>
                            <td className="px-2 py-1.5 text-center">2.64%</td>
                            <td className="px-2 py-1.5 text-center">3.44%</td>
                            <td className="px-2 py-1.5 text-center">2.90%</td>
                            <td className="px-2 py-1.5 text-center">1.29%</td>
                            <td className="px-2 py-1.5 text-center">2.94%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">High Yield</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.high_yield)}</td>
                            <td className="px-2 py-1.5 text-center">1.84%</td>
                            <td className="px-2 py-1.5 text-center">0.84%</td>
                            <td className="px-2 py-1.5 text-center">0.43%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">3.60%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.13%</td>
                            <td className="px-2 py-1.5 text-center">0.48%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Venture Capital</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.venture_capital)}</td>
                            <td className="px-2 py-1.5 text-center">1.70%</td>
                            <td className="px-2 py-1.5 text-center">6.24%</td>
                            <td className="px-2 py-1.5 text-center">9.18%</td>
                            <td className="px-2 py-1.5 text-center">9.31%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">1.04%</td>
                            <td className="px-2 py-1.5 text-center">0.48%</td>
                            <td className="px-2 py-1.5 text-center">4.52%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">8.85%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Low Beta Alpha</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.low_beta_alpha)}</td>
                            <td className="px-2 py-1.5 text-center">1.70%</td>
                            <td className="px-2 py-1.5 text-center">6.24%</td>
                            <td className="px-2 py-1.5 text-center">3.54%</td>
                            <td className="px-2 py-1.5 text-center">3.33%</td>
                            <td className="px-2 py-1.5 text-center">2.63%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">1.47%</td>
                            <td className="px-2 py-1.5 text-center">0.75%</td>
                            <td className="px-2 py-1.5 text-center">2.68%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">16.73%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Equity Derivatives</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.equity_derivatives)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.67%</td>
                            <td className="px-2 py-1.5 text-center">0.87%</td>
                            <td className="px-2 py-1.5 text-center">0.51%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.28%</td>
                            <td className="px-2 py-1.5 text-center">1.28%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Income Notes</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(equities.subcategories.income_notes)}</td>
                            <td className="px-2 py-1.5 text-center">1.88%</td>
                            <td className="px-2 py-1.5 text-center">0.67%</td>
                            <td className="px-2 py-1.5 text-center">0.87%</td>
                            <td className="px-2 py-1.5 text-center">0.51%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.28%</td>
                            <td className="px-2 py-1.5 text-center">1.28%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>

                        {/* FIXED INCOME SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Fixed Income</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">30.00%</td>
                            <td className="px-2 py-1.5 text-center">25.83%</td>
                            <td className="px-2 py-1.5 text-center">26.98%</td>
                            <td className="px-2 py-1.5 text-center">30.43%</td>
                            <td className="px-2 py-1.5 text-center">25.54%</td>
                            <td className="px-2 py-1.5 text-center">27.34%</td>
                            <td className="px-2 py-1.5 text-center">26.43%</td>
                            <td className="px-2 py-1.5 text-center">16.92%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">20.82%</td>
                            <td className="px-2 py-1.5 text-center">14.97%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatNumber(fixed_income.duration) || '-'}</td>
                            <td className="px-2 py-1.5 text-center">5.74</td>
                            <td className="px-2 py-1.5 text-center">2.58</td>
                            <td className="px-2 py-1.5 text-center">2.89</td>
                            <td className="px-2 py-1.5 text-center">5.29</td>
                            <td className="px-2 py-1.5 text-center">3.82</td>
                            <td className="px-2 py-1.5 text-center">4.58</td>
                            <td className="px-2 py-1.5 text-center">3.32</td>
                            <td className="px-2 py-1.5 text-center">2.82</td>
                            <td className="px-2 py-1.5 text-center">2.58</td>
                            <td className="px-2 py-1.5 text-center">0.00</td>
                            <td className="px-2 py-1.5 text-center">3.72</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Municipal Bonds</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.municipal_bonds.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">10.00%</td>
                            <td className="px-2 py-1.5 text-center">7.33%</td>
                            <td className="px-2 py-1.5 text-center">7.56%</td>
                            <td className="px-2 py-1.5 text-center">10.01%</td>
                            <td className="px-2 py-1.5 text-center">6.01%</td>
                            <td className="px-2 py-1.5 text-center">12.28%</td>
                            <td className="px-2 py-1.5 text-center">14.67%</td>
                            <td className="px-2 py-1.5 text-center">10.34%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.32%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Short Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.municipal_bonds.short_duration)}</td>
                            <td className="px-2 py-1.5 text-center">3.84%</td>
                            <td className="px-2 py-1.5 text-center">2.92%</td>
                            <td className="px-2 py-1.5 text-center">2.54%</td>
                            <td className="px-2 py-1.5 text-center">2.74%</td>
                            <td className="px-2 py-1.5 text-center">1.60%</td>
                            <td className="px-2 py-1.5 text-center">2.85%</td>
                            <td className="px-2 py-1.5 text-center">3.24%</td>
                            <td className="px-2 py-1.5 text-center">3.68%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.32%</td>
                            <td className="px-2 py-1.5 text-center">1.64%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Market Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.municipal_bonds.market_duration)}</td>
                            <td className="px-2 py-1.5 text-center">3.98%</td>
                            <td className="px-2 py-1.5 text-center">2.30%</td>
                            <td className="px-2 py-1.5 text-center">1.87%</td>
                            <td className="px-2 py-1.5 text-center">3.54%</td>
                            <td className="px-2 py-1.5 text-center">2.21%</td>
                            <td className="px-2 py-1.5 text-center">2.81%</td>
                            <td className="px-2 py-1.5 text-center">4.62%</td>
                            <td className="px-2 py-1.5 text-center">1.39%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.12%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Long Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.municipal_bonds.long_duration)}</td>
                            <td className="px-2 py-1.5 text-center">0.98%</td>
                            <td className="px-2 py-1.5 text-center">0.24%</td>
                            <td className="px-2 py-1.5 text-center">0.46%</td>
                            <td className="px-2 py-1.5 text-center">0.22%</td>
                            <td className="px-2 py-1.5 text-center">0.48%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">0.31%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.22%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Investment Grade</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.investment_grade.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">9.50%</td>
                            <td className="px-2 py-1.5 text-center">7.49%</td>
                            <td className="px-2 py-1.5 text-center">7.49%</td>
                            <td className="px-2 py-1.5 text-center">10.59%</td>
                            <td className="px-2 py-1.5 text-center">9.69%</td>
                            <td className="px-2 py-1.5 text-center">0.36%</td>
                            <td className="px-2 py-1.5 text-center">0.19%</td>
                            <td className="px-2 py-1.5 text-center">0.34%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">3.90%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Short Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.investment_grade.short_duration)}</td>
                            <td className="px-2 py-1.5 text-center">3.84%</td>
                            <td className="px-2 py-1.5 text-center">2.92%</td>
                            <td className="px-2 py-1.5 text-center">2.54%</td>
                            <td className="px-2 py-1.5 text-center">3.60%</td>
                            <td className="px-2 py-1.5 text-center">1.60%</td>
                            <td className="px-2 py-1.5 text-center">2.85%</td>
                            <td className="px-2 py-1.5 text-center">3.24%</td>
                            <td className="px-2 py-1.5 text-center">3.68%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.32%</td>
                            <td className="px-2 py-1.5 text-center">1.64%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Market Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.investment_grade.market_duration)}</td>
                            <td className="px-2 py-1.5 text-center">3.98%</td>
                            <td className="px-2 py-1.5 text-center">2.30%</td>
                            <td className="px-2 py-1.5 text-center">1.87%</td>
                            <td className="px-2 py-1.5 text-center">3.73%</td>
                            <td className="px-2 py-1.5 text-center">2.23%</td>
                            <td className="px-2 py-1.5 text-center">2.63%</td>
                            <td className="px-2 py-1.5 text-center">4.62%</td>
                            <td className="px-2 py-1.5 text-center">1.14%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.12%</td>
                            <td className="px-2 py-1.5 text-center">1.08%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Long Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.investment_grade.long_duration)}</td>
                            <td className="px-2 py-1.5 text-center">4.50%</td>
                            <td className="px-2 py-1.5 text-center">2.49%</td>
                            <td className="px-2 py-1.5 text-center">10.98%</td>
                            <td className="px-2 py-1.5 text-center">9.61%</td>
                            <td className="px-2 py-1.5 text-center">4.58%</td>
                            <td className="px-2 py-1.5 text-center">0.19%</td>
                            <td className="px-2 py-1.5 text-center">10.09%</td>
                            <td className="px-2 py-1.5 text-center">1.98%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">14.54%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Government Bonds</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.government_bonds.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">9.00%</td>
                            <td className="px-2 py-1.5 text-center">9.66%</td>
                            <td className="px-2 py-1.5 text-center">0.66%</td>
                            <td className="px-2 py-1.5 text-center">3.01%</td>
                            <td className="px-2 py-1.5 text-center">3.03%</td>
                            <td className="px-2 py-1.5 text-center">3.28%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">3.14%</td>
                            <td className="px-2 py-1.5 text-center">0.26%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">3.00%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Short Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.government_bonds.short_duration)}</td>
                            <td className="px-2 py-1.5 text-center">2.65%</td>
                            <td className="px-2 py-1.5 text-center">4.98%</td>
                            <td className="px-2 py-1.5 text-center">0.66%</td>
                            <td className="px-2 py-1.5 text-center">1.03%</td>
                            <td className="px-2 py-1.5 text-center">3.03%</td>
                            <td className="px-2 py-1.5 text-center">2.86%</td>
                            <td className="px-2 py-1.5 text-center">1.24%</td>
                            <td className="px-2 py-1.5 text-center">1.88%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.08%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Market Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.government_bonds.market_duration)}</td>
                            <td className="px-2 py-1.5 text-center">2.85%</td>
                            <td className="px-2 py-1.5 text-center">1.87%</td>
                            <td className="px-2 py-1.5 text-center">2.79%</td>
                            <td className="px-2 py-1.5 text-center">1.79%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.63%</td>
                            <td className="px-2 py-1.5 text-center">4.62%</td>
                            <td className="px-2 py-1.5 text-center">1.14%</td>
                            <td className="px-2 py-1.5 text-center">0.76%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.23%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-8">Long Duration</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.government_bonds.long_duration)}</td>
                            <td className="px-2 py-1.5 text-center">2.85%</td>
                            <td className="px-2 py-1.5 text-center">2.97%</td>
                            <td className="px-2 py-1.5 text-center">1.76%</td>
                            <td className="px-2 py-1.5 text-center">1.75%</td>
                            <td className="px-2 py-1.5 text-center">3.28%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">1.16%</td>
                            <td className="px-2 py-1.5 text-center">0.56%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.23%</td>
                            <td className="px-2 py-1.5 text-center">1.08%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Fixed Income Derivatives</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(fixed_income.subcategories.fixed_income_derivatives.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">1.51%</td>
                            <td className="px-2 py-1.5 text-center">1.31%</td>
                            <td className="px-2 py-1.5 text-center">1.31%</td>
                            <td className="px-2 py-1.5 text-center">1.71%</td>
                            <td className="px-2 py-1.5 text-center">1.75%</td>
                            <td className="px-2 py-1.5 text-center">1.42%</td>
                            <td className="px-2 py-1.5 text-center">1.42%</td>
                            <td className="px-2 py-1.5 text-center">1.74%</td>
                            <td className="px-2 py-1.5 text-center">0.25%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.62%</td>
                        </tr>
                        
                        {/* HARD CURRENCY SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Hard Currency</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">22.00%</td>
                            <td className="px-2 py-1.5 text-center">17.38%</td>
                            <td className="px-2 py-1.5 text-center">20.98%</td>
                            <td className="px-2 py-1.5 text-center">15.15%</td>
                            <td className="px-2 py-1.5 text-center">16.56%</td>
                            <td className="px-2 py-1.5 text-center">21.38%</td>
                            <td className="px-2 py-1.5 text-center">19.78%</td>
                            <td className="px-2 py-1.5 text-center">16.21%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">19.46%</td>
                            <td className="px-2 py-1.5 text-center">20.00%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Gold</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.gold)}</td>
                            <td className="px-2 py-1.5 text-center">20.00%</td>
                            <td className="px-2 py-1.5 text-center">20.58%</td>
                            <td className="px-2 py-1.5 text-center">21.62%</td>
                            <td className="px-2 py-1.5 text-center">20.24%</td>
                            <td className="px-2 py-1.5 text-center">8.58%</td>
                            <td className="px-2 py-1.5 text-center">23.64%</td>
                            <td className="px-2 py-1.5 text-center">7.43%</td>
                            <td className="px-2 py-1.5 text-center">20.00%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">3.62%</td>
                            <td className="px-2 py-1.5 text-center">20.00%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Silver</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.silver)}</td>
                            <td className="px-2 py-1.5 text-center">2.00%</td>
                            <td className="px-2 py-1.5 text-center">8.97%</td>
                            <td className="px-2 py-1.5 text-center">12.11%</td>
                            <td className="px-2 py-1.5 text-center">8.46%</td>
                            <td className="px-2 py-1.5 text-center">3.24%</td>
                            <td className="px-2 py-1.5 text-center">12.00%</td>
                            <td className="px-2 py-1.5 text-center">7.38%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Gold Miners</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.gold_miners)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.39%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">0.02%</td>
                            <td className="px-2 py-1.5 text-center">0.52%</td>
                            <td className="px-2 py-1.5 text-center">0.01%</td>
                            <td className="px-2 py-1.5 text-center">0.31%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Silver Miners</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.silver_miners)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Industrial Metals</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.industrial_metals)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Hard Currency Physical Investment</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.hard_currency_physical_investment)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.49%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.07%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.59%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Precious Metals Derivatives</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(hard_currency.subcategories.precious_metals_derivatives)}</td>
                            <td className="px-2 py-1.5 text-center">6.00%</td>
                            <td className="px-2 py-1.5 text-center">5.89%</td>
                            <td className="px-2 py-1.5 text-center">4.69%</td>
                            <td className="px-2 py-1.5 text-center">4.94%</td>
                            <td className="px-2 py-1.5 text-center">9.31%</td>
                            <td className="px-2 py-1.5 text-center">3.67%</td>
                            <td className="px-2 py-1.5 text-center">0.64%</td>
                            <td className="px-2 py-1.5 text-center">6.41%</td>
                            <td className="px-2 py-1.5 text-center">5.27%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.03%</td>
                        </tr>

                        {/* UNCORRELATED ALTERNATIVES SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Uncorrelated Alternatives</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(uncorrelated_alternatives.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">16.00%</td>
                            <td className="px-2 py-1.5 text-center">6.21%</td>
                            <td className="px-2 py-1.5 text-center">3.19%</td>
                            <td className="px-2 py-1.5 text-center">9.10%</td>
                            <td className="px-2 py-1.5 text-center">9.45%</td>
                            <td className="px-2 py-1.5 text-center">6.00%</td>
                            <td className="px-2 py-1.5 text-center">3.15%</td>
                            <td className="px-2 py-1.5 text-center">14.05%</td>
                            <td className="px-2 py-1.5 text-center">0.92%</td>
                            <td className="px-2 py-1.5 text-center">9.86%</td>
                            <td className="px-2 py-1.5 text-center">16.52%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Crypto</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(uncorrelated_alternatives.subcategories.crypto)}</td>
                            <td className="px-2 py-1.5 text-center">3.00%</td>
                            <td className="px-2 py-1.5 text-center">3.34%</td>
                            <td className="px-2 py-1.5 text-center">0.11%</td>
                            <td className="px-2 py-1.5 text-center">0.21%</td>
                            <td className="px-2 py-1.5 text-center">2.40%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.28%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.26%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.72%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">CTAs</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(uncorrelated_alternatives.subcategories.ctas)}</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">2.21%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">1.22%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.24%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">0.28%</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Periodic Short Term Alt Fund</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(uncorrelated_alternatives.subcategories.periodic_short_term_alt_fund)}</td>
                            <td className="px-2 py-1.5 text-center">3.00%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">1.22%</td>
                            <td className="px-2 py-1.5 text-center">1.44%</td>
                            <td className="px-2 py-1.5 text-center">4.15%</td>
                            <td className="px-2 py-1.5 text-center">1.97%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.98%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">2.64%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Periodic Long Term Alt Fund</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(uncorrelated_alternatives.subcategories.periodic_long_term_alt_fund)}</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">2.00%</td>
                            <td className="px-2 py-1.5 text-center">1.37%</td>
                            <td className="px-2 py-1.5 text-center">1.44%</td>
                            <td className="px-2 py-1.5 text-center">4.13%</td>
                            <td className="px-2 py-1.5 text-center">1.97%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.98%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                        </tr>

                        {/* CASH SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Cash & Cash Equivalents</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(cash.total_pct)}</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">6.62%</td>
                            <td className="px-2 py-1.5 text-center">1.49%</td>
                            <td className="px-2 py-1.5 text-center">1.00%</td>
                            <td className="px-2 py-1.5 text-center">1.68%</td>
                            <td className="px-2 py-1.5 text-center">1.60%</td>
                            <td className="px-2 py-1.5 text-center">4.71%</td>
                            <td className="px-2 py-1.5 text-center">1.61%</td>
                            <td className="px-2 py-1.5 text-center">99.08%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">12.48%</td>
                        </tr>

                        {/* LIQUIDITY SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Liquidity %</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Liquid Assets</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(liquidity.liquid_assets)}</td>
                            <td className="px-2 py-1.5 text-center">66.21%</td>
                            <td className="px-2 py-1.5 text-center">72.24%</td>
                            <td className="px-2 py-1.5 text-center">73.30%</td>
                            <td className="px-2 py-1.5 text-center">77.44%</td>
                            <td className="px-2 py-1.5 text-center">80.47%</td>
                            <td className="px-2 py-1.5 text-center">64.33%</td>
                            <td className="px-2 py-1.5 text-center">84.39%</td>
                            <td className="px-2 py-1.5 text-center">84.33%</td>
                            <td className="px-2 py-1.5 text-center">99.08%</td>
                            <td className="px-2 py-1.5 text-center">64.11%</td>
                            <td className="px-2 py-1.5 text-center">100.00%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">Illiquid Assets %</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(liquidity.illiquid_assets)}</td>
                            <td className="px-2 py-1.5 text-center">33.49%</td>
                            <td className="px-2 py-1.5 text-center">27.76%</td>
                            <td className="px-2 py-1.5 text-center">26.85%</td>
                            <td className="px-2 py-1.5 text-center">22.56%</td>
                            <td className="px-2 py-1.5 text-center">19.53%</td>
                            <td className="px-2 py-1.5 text-center">35.67%</td>
                            <td className="px-2 py-1.5 text-center">15.61%</td>
                            <td className="px-2 py-1.5 text-center">15.67%</td>
                            <td className="px-2 py-1.5 text-center">0.92%</td>
                            <td className="px-2 py-1.5 text-center">35.89%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                        </tr>

                        {/* PERFORMANCE SECTION */}
                        <tr className="border-t-2 border-gray-300">
                            <td className="px-2 py-1.5 text-left font-medium">Performance</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                            <td className="px-2 py-1.5 text-center">-</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">1D</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(performance['1D'])}</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.20%</td>
                            <td className="px-2 py-1.5 text-center">0.33%</td>
                            <td className="px-2 py-1.5 text-center">0.49%</td>
                            <td className="px-2 py-1.5 text-center">0.20%</td>
                            <td className="px-2 py-1.5 text-center">0.17%</td>
                            <td className="px-2 py-1.5 text-center">0.09%</td>
                            <td className="px-2 py-1.5 text-center">0.47%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.50%</td>
                            <td className="px-2 py-1.5 text-center">0.26%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">MTD</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(performance['MTD'])}</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.50%</td>
                            <td className="px-2 py-1.5 text-center">0.53%</td>
                            <td className="px-2 py-1.5 text-center">0.97%</td>
                            <td className="px-2 py-1.5 text-center">0.20%</td>
                            <td className="px-2 py-1.5 text-center">0.61%</td>
                            <td className="px-2 py-1.5 text-center">0.48%</td>
                            <td className="px-2 py-1.5 text-center">0.42%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.53%</td>
                            <td className="px-2 py-1.5 text-center">0.57%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">QTD</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(performance['QTD'])}</td>
                            <td className="px-2 py-1.5 text-center">1.64%</td>
                            <td className="px-2 py-1.5 text-center">1.04%</td>
                            <td className="px-2 py-1.5 text-center">2.17%</td>
                            <td className="px-2 py-1.5 text-center">0.77%</td>
                            <td className="px-2 py-1.5 text-center">1.04%</td>
                            <td className="px-2 py-1.5 text-center">0.57%</td>
                            <td className="px-2 py-1.5 text-center">1.04%</td>
                            <td className="px-2 py-1.5 text-center">1.87%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">0.50%</td>
                            <td className="px-2 py-1.5 text-center">1.31%</td>
                        </tr>
                        <tr>
                            <td className="px-2 py-1.5 text-left pl-4">YTD</td>
                            <td className="px-2 py-1.5 text-center">{formatPercent(performance['YTD'])}</td>
                            <td className="px-2 py-1.5 text-center">2.51%</td>
                            <td className="px-2 py-1.5 text-center">2.01%</td>
                            <td className="px-2 py-1.5 text-center">2.53%</td>
                            <td className="px-2 py-1.5 text-center">3.31%</td>
                            <td className="px-2 py-1.5 text-center">3.97%</td>
                            <td className="px-2 py-1.5 text-center">0.00%</td>
                            <td className="px-2 py-1.5 text-center">1.96%</td>
                            <td className="px-2 py-1.5 text-center">3.52%</td>
                            <td className="px-2 py-1.5 text-center">1.12%</td>
                            <td className="px-2 py-1.5 text-center">2.69%</td>
                            <td className="px-2 py-1.5 text-center">4.42%</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            {/* Portfolio Total Value Display */}
            <div className="mt-8 text-center">
                <div className="text-gray-600 text-sm">Portfolio Adjusted Value</div>
                <div className="text-2xl font-semibold text-green-800">{formatCurrency(total_adjusted_value)}</div>
            </div>
        </div>
    );
};

// No export needed with window assignment