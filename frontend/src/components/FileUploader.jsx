const FileUploader = ({ title, description, endpoint, icon }) => {
    const [file, setFile] = React.useState(null);
    const [isUploading, setIsUploading] = React.useState(false);
    const [uploadProgress, setUploadProgress] = React.useState(0);
    const [uploadResult, setUploadResult] = React.useState(null);
    const [error, setError] = React.useState('');
    
    const fileInputRef = React.useRef(null);
    
    // Handle file selection
    const handleFileChange = (event) => {
        const selectedFile = event.target.files[0];
        
        // Reset states
        setFile(selectedFile);
        setUploadResult(null);
        setError('');
        
        // Validate file type
        if (selectedFile && !selectedFile.name.endsWith('.xlsx')) {
            setError('Only Excel (.xlsx) files are accepted');
            setFile(null);
        }
    };
    
    // Handle file upload
    const handleUpload = async () => {
        if (!file) {
            setError('Please select a file first');
            return;
        }
        
        setIsUploading(true);
        setUploadProgress(0);
        setError('');
        
        try {
            // Create form data
            const formData = new FormData();
            formData.append('file', file);
            
            // Simulate upload progress
            const progressInterval = setInterval(() => {
                setUploadProgress((prev) => {
                    const nextProgress = prev + 5;
                    return nextProgress > 90 ? 90 : nextProgress;
                });
            }, 200);
            
            // Send the file to the API
            const response = await api.uploadFile(endpoint, formData);
            
            // Complete the progress
            clearInterval(progressInterval);
            setUploadProgress(100);
            
            // Set the result
            setUploadResult(response);
            
            // Reset file input
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
            setFile(null);
            
        } catch (err) {
            console.error('Upload error:', err);
            setError(err.response?.data?.detail || 'An error occurred during upload');
        } finally {
            setIsUploading(false);
        }
    };
    
    return (
        <div className="bg-white rounded-lg shadow-md p-6">
            <div className="flex items-center mb-4">
                <div className="w-10 h-10 rounded-full bg-green-100 flex items-center justify-center text-green-700 mr-3">
                    <i className={`fas ${icon || 'fa-file-upload'}`}></i>
                </div>
                <h3 className="text-lg font-semibold text-gray-800">{title}</h3>
            </div>
            
            <p className="text-sm text-gray-600 mb-4">{description}</p>
            
            <div className="mb-4">
                <input
                    type="file"
                    accept=".xlsx"
                    onChange={handleFileChange}
                    className="hidden"
                    id={`file-${title.replace(/\s+/g, '-').toLowerCase()}`}
                    ref={fileInputRef}
                    disabled={isUploading}
                />
                <label
                    htmlFor={`file-${title.replace(/\s+/g, '-').toLowerCase()}`}
                    className="cursor-pointer bg-gray-100 hover:bg-gray-200 text-gray-700 font-medium py-2 px-4 rounded-lg inline-flex items-center w-full mb-2"
                >
                    <i className="fas fa-file-excel mr-2"></i>
                    <span>{file ? file.name : 'Select Excel File'}</span>
                </label>
                
                <button
                    onClick={handleUpload}
                    disabled={!file || isUploading}
                    className={`w-full py-2 px-4 rounded-lg font-medium ${
                        !file || isUploading
                            ? 'bg-gray-300 text-gray-600 cursor-not-allowed'
                            : 'bg-green-700 text-white hover:bg-green-800'
                    }`}
                >
                    {isUploading ? (
                        <span className="flex items-center justify-center">
                            <i className="fas fa-spinner fa-spin mr-2"></i>
                            Uploading...
                        </span>
                    ) : (
                        <span className="flex items-center justify-center">
                            <i className="fas fa-upload mr-2"></i>
                            Upload
                        </span>
                    )}
                </button>
            </div>
            
            {isUploading && (
                <div className="mb-4">
                    <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                        <div
                            className="h-full bg-green-600 rounded-full transition-all duration-300"
                            style={{ width: `${uploadProgress}%` }}
                        ></div>
                    </div>
                    <div className="text-xs text-gray-500 mt-1 text-right">
                        {uploadProgress}%
                    </div>
                </div>
            )}
            
            {error && (
                <div className="text-red-600 text-sm mb-4 p-2 bg-red-50 rounded-md">
                    <i className="fas fa-exclamation-circle mr-1"></i>
                    {error}
                </div>
            )}
            
            {uploadResult && (
                <div className={`text-sm mb-1 p-2 rounded-md ${uploadResult.success ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-600'}`}>
                    <div className="font-medium mb-1">
                        <i className={`fas ${uploadResult.success ? 'fa-check-circle' : 'fa-exclamation-circle'} mr-1`}></i>
                        {uploadResult.message}
                    </div>
                    <div className="text-xs">
                        Processed: {uploadResult.rows_processed} rows<br />
                        Inserted: {uploadResult.rows_inserted} rows
                    </div>
                    {uploadResult.errors && uploadResult.errors.length > 0 && (
                        <div className="mt-2">
                            <p className="text-xs font-medium">Errors:</p>
                            <ul className="text-xs list-disc pl-4 mt-1">
                                {uploadResult.errors.map((err, i) => (
                                    <li key={i}>{err}</li>
                                ))}
                            </ul>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};
