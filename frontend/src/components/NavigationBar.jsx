const { Link, useLocation } = ReactRouterDOM;

const NavigationBar = () => {
    const location = useLocation();
    const [isMobileMenuOpen, setIsMobileMenuOpen] = React.useState(false);
    
    // Navigation items
    const navItems = [
        { name: 'Dashboard', path: '/', icon: 'fa-tachometer-alt' },
        { name: 'Upload Files', path: '/upload', icon: 'fa-upload' },
        { name: 'Ownership Tree', path: '/ownership', icon: 'fa-sitemap' },
        { name: 'Reports', path: '/reports', icon: 'fa-file-text' },
        { name: 'Risk Stats', path: '/risk-stats', icon: 'fa-chart-line' },
        { name: 'Model Portfolios', path: '/model-portfolios', icon: 'fa-balance-scale' }
    ];
    
    // Check if a path is active
    const isActive = (path) => {
        return location.pathname === path;
    };
    
    return (
        <nav className="bg-green-900 text-white shadow-md">
            <div className="container mx-auto px-4">
                <div className="flex justify-between items-center h-16">
                    {/* Logo and brand */}
                    <div className="flex items-center">
                        <div className="flex-shrink-0 font-bold text-xl tracking-tight">
                            <Link to="/" className="flex items-center">
                                <span className="text-white text-2xl font-bold">NORI</span>
                            </Link>
                        </div>
                    </div>
                    
                    {/* Desktop navigation */}
                    <div className="hidden md:block">
                        <div className="ml-10 flex items-center space-x-4">
                            {navItems.map((item) => (
                                <Link
                                    key={item.path}
                                    to={item.path}
                                    className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                                        isActive(item.path)
                                            ? 'bg-green-800 text-white'
                                            : 'text-gray-200 hover:bg-green-800 hover:text-white'
                                    }`}
                                >
                                    <i className={`fas ${item.icon} mr-2`}></i>
                                    {item.name}
                                </Link>
                            ))}
                        </div>
                    </div>
                    
                    {/* Mobile menu button */}
                    <div className="md:hidden">
                        <button
                            onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                            className="inline-flex items-center justify-center p-2 rounded-md text-gray-200 hover:text-white hover:bg-green-800 focus:outline-none"
                        >
                            <i className={`fas ${isMobileMenuOpen ? 'fa-times' : 'fa-bars'}`}></i>
                        </button>
                    </div>
                </div>
            </div>
            
            {/* Mobile menu */}
            <div className={`md:hidden ${isMobileMenuOpen ? 'block' : 'hidden'}`}>
                <div className="px-2 pt-2 pb-3 space-y-1 sm:px-3">
                    {navItems.map((item) => (
                        <Link
                            key={item.path}
                            to={item.path}
                            className={`block px-3 py-2 rounded-md text-base font-medium ${
                                isActive(item.path)
                                    ? 'bg-green-800 text-white'
                                    : 'text-gray-200 hover:bg-green-800 hover:text-white'
                            }`}
                            onClick={() => setIsMobileMenuOpen(false)}
                        >
                            <i className={`fas ${item.icon} mr-2`}></i>
                            {item.name}
                        </Link>
                    ))}
                </div>
            </div>
        </nav>
    );
};
