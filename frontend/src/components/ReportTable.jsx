const ReportTable = ({ data, title, columns }) => {
    // Default columns if none provided
    const defaultColumns = [
        { id: 'name', label: 'Name', accessor: 'name' },
        { id: 'value', label: 'Value', accessor: 'value', format: (value) => `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
        { id: 'percentage', label: '%', accessor: 'percentage', format: (value) => `${Number(value).toFixed(2)}%` }
    ];
    
    const tableColumns = columns || defaultColumns;
    
    // Handle empty data
    if (!data || data.length === 0) {
        return (
            <div className="bg-white rounded-lg shadow-md p-6 mb-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">{title || 'Report Data'}</h3>
                <div className="text-center py-8 text-gray-500">
                    <i className="fas fa-info-circle text-2xl mb-2"></i>
                    <p>No data available.</p>
                </div>
            </div>
        );
    }
    
    return (
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">{title || 'Report Data'}</h3>
            <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                        <tr>
                            {tableColumns.map((column) => (
                                <th
                                    key={column.id}
                                    scope="col"
                                    className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                                >
                                    {column.label}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                        {data.map((item, index) => (
                            <tr key={index} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                                {tableColumns.map((column) => {
                                    const value = typeof column.accessor === 'function' 
                                        ? column.accessor(item) 
                                        : item[column.accessor];
                                    
                                    const formattedValue = column.format 
                                        ? column.format(value) 
                                        : value;
                                    
                                    return (
                                        <td
                                            key={column.id}
                                            className="px-6 py-4 whitespace-nowrap text-sm text-gray-700"
                                        >
                                            {formattedValue}
                                        </td>
                                    );
                                })}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};
