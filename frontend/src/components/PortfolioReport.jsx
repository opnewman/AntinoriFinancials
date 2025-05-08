// Professional portfolio report component to match exactly the Excel template format
window.PortfolioReport = ({
    reportData,
    loading,
    displayFormat = "percent",
}) => {
    const formatDate = (dateStr) => {
        if (!dateStr) return "";
        const date = new Date(dateStr);
        return date.toLocaleDateString("en-US", {
            month: "2-digit",
            day: "2-digit",
            year: "numeric",
        });
    };

    // Format percentage for display
    const formatPercent = (value) => {
        if (value === null || value === undefined) return "";
        return parseFloat(value).toFixed(2) + "%";
    };

    // Format currency for display
    const formatCurrency = (value) => {
        if (value === null || value === undefined) return "";
        return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        }).format(value);
    };

    // Format number with 2 decimal places
    const formatNumber = (value) => {
        if (value === null || value === undefined) return "";
        return parseFloat(value).toFixed(2);
    };

    // Format value based on display format (percent or dollar)
    const formatValue = (percentValue) => {
        if (percentValue === null || percentValue === undefined) return "";

        if (displayFormat === "dollar" && total_adjusted_value) {
            // Convert percentage to dollar amount
            const dollarValue = (percentValue * total_adjusted_value) / 100;
            return formatCurrency(dollarValue);
        } else {
            // Display as percentage
            return formatPercent(percentValue);
        }
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center h-64">
                <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-green-800"></div>
                <span className="ml-4 text-lg font-medium text-gray-600">
                    Loading portfolio report...
                </span>
            </div>
        );
    }

    if (!reportData) {
        return (
            <div className="bg-white p-6 rounded-lg shadow-md text-center">
                <i className="fas fa-file-excel text-4xl text-gray-400 mb-4"></i>
                <h3 className="text-xl font-medium text-gray-700">
                    No Report Data
                </h3>
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
        performance,
    } = reportData;

    return (
        <div className="portfolio-report bg-white p-6 rounded-lg shadow-md overflow-x-auto">
            {/* Report header */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-blue-800 text-white px-4 py-2 text-left font-medium">
                            Portfolio
                        </td>
                        <td className="border px-4 py-2">{portfolio || ""}</td>
                    </tr>
                    <tr>
                        <td className="bg-blue-800 text-white px-4 py-2 text-left font-medium">
                            PM
                        </td>
                        <td className="border px-4 py-2">Justin</td>
                    </tr>
                    <tr>
                        <td className="bg-blue-800 text-white px-4 py-2 text-left font-medium">
                            Classification
                        </td>
                        <td className="border px-4 py-2"></td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Equity Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-blue-200 px-4 py-2 text-left font-medium">
                            Equity
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.total_pct)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-blue-50 font-medium">
                            Vol
                        </td>
                        <td className="border px-4 py-2 text-right bg-blue-50">
                            {formatNumber(equities.vol || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-blue-50 font-medium">
                            Beta
                        </td>
                        <td className="border px-4 py-2 text-right bg-blue-50">
                            {formatNumber(equities.beta || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-blue-50 font-medium">
                            Beta Adjusted
                        </td>
                        <td className="border px-4 py-2 text-right bg-blue-50">
                            {formatNumber(equities.beta_adjusted || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            US Markets
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.us_markets)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Global Markets
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.global_markets)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Emerging Markets
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                equities.subcategories.emerging_markets,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Commodities
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.commodities)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Real Estate
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.real_estate)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Private Equity
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.private_equity)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            High Yield
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.high_yield)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Venture Capital
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                equities.subcategories.venture_capital,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Low Beta Alpha
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.low_beta_alpha)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Equity Derivatives
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                equities.subcategories.equity_derivatives,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Income Notes
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(equities.subcategories.income_notes)}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Fixed Income Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-red-200 px-4 py-2 text-left font-medium">
                            Fixed Income
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(fixed_income.total_pct)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-red-50 font-medium">
                            Duration
                        </td>
                        <td className="border px-4 py-2 text-right bg-red-50">
                            {formatNumber(fixed_income.duration || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Municipal Bonds
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.municipal_bonds
                                    .total_pct,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Low Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.municipal_bonds
                                    .short_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Market Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.municipal_bonds
                                    .market_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Long Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.municipal_bonds
                                    .long_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Investment Grade
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.investment_grade
                                    .total_pct,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Low Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.investment_grade
                                    .short_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Market Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.investment_grade
                                    .market_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Long Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.investment_grade
                                    .long_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Government Bonds
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.government_bonds
                                    .total_pct,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Low Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.government_bonds
                                    .short_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Market Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.government_bonds
                                    .market_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-10">
                            Long Duration
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories.government_bonds
                                    .long_duration,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Fixed Income Derivatives
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                fixed_income.subcategories
                                    .fixed_income_derivatives.total_pct,
                            )}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Hard Currency Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-yellow-200 px-4 py-2 text-left font-medium">
                            Hard Currency
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(hard_currency.total_pct)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-yellow-50 font-medium">
                            HC Beta
                        </td>
                        <td className="border px-4 py-2 text-right bg-yellow-50">
                            {formatNumber(hard_currency.beta || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6 bg-yellow-50 font-medium">
                            Beta adj.
                        </td>
                        <td className="border px-4 py-2 text-right bg-yellow-50">
                            {formatNumber(hard_currency.beta_adjusted || "")}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Gold
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(hard_currency.subcategories.gold)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Gold Miners
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                hard_currency.subcategories.gold_miners,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Silver
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(hard_currency.subcategories.silver)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Silver Miners
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                hard_currency.subcategories.silver_miners,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Industrial Metals
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                hard_currency.subcategories.industrial_metals,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Hard Currency Private Investment
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                hard_currency.subcategories
                                    .hard_currency_physical_investment,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Precious Metals Derivatives
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                hard_currency.subcategories
                                    .precious_metals_derivatives,
                            )}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Uncorrelated Alternatives Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-amber-200 px-4 py-2 text-left font-medium">
                            Uncorrelated Alternatives
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(uncorrelated_alternatives.total_pct)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Crypto
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                uncorrelated_alternatives.subcategories.crypto,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Other
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                uncorrelated_alternatives.subcategories.other,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Proficio Short Term Alts Fund
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                uncorrelated_alternatives.subcategories
                                    .proficio_short_term,
                            )}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Proficio Long Term Alts Fund
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(
                                uncorrelated_alternatives.subcategories
                                    .proficio_long_term,
                            )}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Cash Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-green-200 px-4 py-2 text-left font-medium">
                            Cash & Cash Equivalent
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(cash.total_pct)}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Liquidity Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-gray-200 px-4 py-2 text-left font-medium">
                            Liquidity %
                        </td>
                        <td className="border px-4 py-2 text-right"></td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Liquid Asset %
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(liquidity.liquid_assets)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">
                            Illiquid Asset %
                        </td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(liquidity.illiquid_assets)}
                        </td>
                    </tr>
                </tbody>
            </table>

            <div className="mt-4"></div>

            {/* Performance Section */}
            <table className="min-w-full text-sm border-collapse">
                <tbody>
                    <tr>
                        <td className="bg-purple-200 px-4 py-2 text-left font-medium">
                            Performance:
                        </td>
                        <td className="border px-4 py-2 text-right"></td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">1D</td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(performance["1D"])}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">MTD</td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(performance.MTD)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">QTD</td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(performance.QTD)}
                        </td>
                    </tr>
                    <tr>
                        <td className="border px-4 py-2 text-left pl-6">YTD</td>
                        <td className="border px-4 py-2 text-right">
                            {formatValue(performance.YTD)}
                        </td>
                    </tr>
                </tbody>
            </table>

            {/* Footer with total value */}
            <div className="mt-4 text-right text-gray-700">
                <span className="font-medium">Total Adjusted Value: </span>
                {formatCurrency(total_adjusted_value)}
            </div>
        </div>
    );
};
