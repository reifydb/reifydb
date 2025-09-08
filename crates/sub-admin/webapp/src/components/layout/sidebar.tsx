import {Link, useLocation, useNavigate} from '@tanstack/react-router'
import {
    Database,
    Settings,
    Users,
    Shield,
    Terminal,
    ChevronLeft,
    ChevronRight,
    LayoutDashboard
} from 'lucide-react'
import {cn} from '@/lib/utils'
import {Button} from '@/components/ui/button'
import {useState} from 'react'

const navigation = [
    {name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard},
    {name: 'Schema', href: '/schema', icon: Database},
    {name: 'Query', href: '/query', icon: Terminal},
    {name: 'Users', href: '/users', icon: Users},
    {name: 'Settings', href: '/settings', icon: Settings},
]

export function Sidebar() {
    const location = useLocation()
    const navigate = useNavigate()
    const [collapsed, setCollapsed] = useState(false)

    return (
        <div className={cn(
            "relative flex flex-col h-full bg-secondary text-secondary-foreground border-r border-border transition-all duration-300",
            collapsed ? "w-20" : "w-64"
        )}>
            <div className={cn(
                "flex items-center p-4 border-b",
                collapsed ? "justify-center" : "justify-between"
            )}>
                <div
                    className="flex items-center gap-2 cursor-pointer hover:opacity-80 transition-opacity"
                    onClick={() => navigate({to: '/dashboard'})}
                >
                    {!collapsed && (
                        <img
                            src="/assets/favicon-128x128.png"
                            alt="ReifyDB Logo"
                            className={cn(
                                "flex-shrink-0 object-contain h-12 w-12",
                            )}
                        />
                    )}
                    {!collapsed && <span className="font-semibold text-lg">ReifyDB</span>}
                </div>
                {!collapsed && (
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setCollapsed(!collapsed)}
                        className="ml-2"
                    >
                        <ChevronLeft className="h-8 w-8"/>
                    </Button>
                )}
                {collapsed && (
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setCollapsed(!collapsed)}
                        className="hover:bg-accent hover:text-accent-foreground transition-colors"
                    >
                        <ChevronRight className="h-8 w-8"/>
                    </Button>
                )}
            </div>

            <nav className="flex-1 p-2 space-y-1">
                {navigation.map((item) => {
                    const isActive = location.pathname === item.href ||
                        (item.href === '/dashboard' && location.pathname === '/')
                    return (
                        <Link
                            key={item.name}
                            to={item.href}
                            className={cn(
                                "flex items-center gap-3 px-3 py-2 text-sm font-medium transition-all duration-200 group relative",
                                isActive
                                    ? "bg-primary text-primary-foreground"
                                    : "text-secondary-foreground/70 hover:text-secondary-foreground hover:bg-secondary-foreground/10",
                                collapsed && "justify-center px-2"
                            )}
                        >
                            <item.icon className={cn(
                                "h-4 w-4 flex-shrink-0 transition-transform duration-200",
                                !isActive && "group-hover:scale-110"
                            )}/>
                            {!collapsed && <span>{item.name}</span>}
                            {collapsed && (
                                <div
                                    className="absolute left-full ml-2 px-2 py-1 bg-popover text-popover-foreground shadow-lg whitespace-nowrap opacity-0 pointer-events-none group-hover:opacity-100 transition-opacity z-50">
                                    {item.name}
                                </div>
                            )}
                        </Link>
                    )
                })}
            </nav>

            <div className="p-4 border-t">
                <div className={cn(
                    "flex items-center gap-3",
                    collapsed && "justify-center"
                )}>
                    <div className="h-8 w-8 bg-primary/10 flex items-center justify-center">
                        <Shield className="h-4 w-4 text-primary"/>
                    </div>
                    {!collapsed && (
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium truncate">Admin</p>
                            <p className="text-xs text-muted-foreground truncate">admin</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}