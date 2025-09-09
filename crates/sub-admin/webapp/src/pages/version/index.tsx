import { useVersion } from '@/hooks/use-version'

export function VersionPage() {
    const [isLoading, versions, error] = useVersion()
    const { version, modules, subsystems, builds } = versions
    
    return (
        <div className="p-6">
            <h1 className="text-2xl font-bold mb-4">Version Information</h1>
            
            {isLoading ? (
                <div className="flex items-center justify-center py-8">
                    <div className="text-muted-foreground">Loading version information...</div>
                </div>
            ) : error ? (
                <div className="bg-destructive/10 text-destructive p-4 rounded-lg">
                    Error loading version information: {error}
                </div>
            ) : (
                <div className="space-y-6">
                    <div className="rounded-lg border border-border bg-gradient-to-br from-primary/5 to-primary/10 p-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                                    ReifyDB Version
                                </h3>
                                <div className="flex items-baseline gap-3">
                                    <span className="text-3xl font-bold text-primary">
                                        {version}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    {modules.length > 0 && (
                    <div className="rounded-lg border border-border overflow-hidden">
                        <div className="px-6 py-4 bg-muted/30 border-b border-border">
                            <h2 className="text-lg font-semibold">Modules</h2>
                        </div>
                        <div className="overflow-x-auto">
                            <table className="w-full table-fixed">
                                <colgroup>
                                    <col className="w-[25%]"/>
                                    <col className="w-[20%]"/>
                                    <col className="w-[55%]"/>
                                </colgroup>
                                <thead>
                                    <tr className="border-b border-border bg-muted/20">
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Name</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Version</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Description</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-border">
                                    {modules.map((row, index) => (
                                        <tr key={index} className="hover:bg-muted/10 transition-colors">
                                            <td className="px-6 py-4">
                                                <span className="font-medium text-sm">{row.name}</span>
                                            </td>
                                            <td className="px-6 py-4">
                                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-md bg-primary/10 text-primary font-mono text-xs font-medium">
                                                    {row.version}
                                                </span>
                                            </td>
                                            <td className="px-6 py-4 text-sm text-muted-foreground">
                                                {row.description}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    )}
                    
                    {subsystems.length > 0 && (
                    <div className="rounded-lg border border-border overflow-hidden">
                        <div className="px-6 py-4 bg-muted/30 border-b border-border">
                            <h2 className="text-lg font-semibold">Subsystems</h2>
                        </div>
                        <div className="overflow-x-auto">
                            <table className="w-full table-fixed">
                                <colgroup>
                                    <col className="w-[25%]"/>
                                    <col className="w-[20%]"/>
                                    <col className="w-[55%]"/>
                                </colgroup>
                                <thead>
                                    <tr className="border-b border-border bg-muted/20">
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Name</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Version</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Description</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-border">
                                    {subsystems.map((row, index) => (
                                        <tr key={index} className="hover:bg-muted/10 transition-colors">
                                            <td className="px-6 py-4">
                                                <span className="font-medium text-sm">{row.name}</span>
                                            </td>
                                            <td className="px-6 py-4">
                                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-md bg-primary/10 text-primary font-mono text-xs font-medium">
                                                    {row.version}
                                                </span>
                                            </td>
                                            <td className="px-6 py-4 text-sm text-muted-foreground">
                                                {row.description}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    )}
                    
                    {builds.length > 0 && (
                    <div className="rounded-lg border border-border overflow-hidden">
                        <div className="px-6 py-4 bg-muted/30 border-b border-border">
                            <h2 className="text-lg font-semibold">Build Information</h2>
                        </div>
                        <div className="overflow-x-auto">
                            <table className="w-full table-fixed">
                                <colgroup>
                                    <col className="w-[25%]"/>
                                    <col className="w-[20%]"/>
                                    <col className="w-[55%]"/>
                                </colgroup>
                                <thead>
                                    <tr className="border-b border-border bg-muted/20">
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Component</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Version</th>
                                        <th className="text-left px-6 py-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Description</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-border">
                                    {builds.map((row, index) => (
                                        <tr key={index} className="hover:bg-muted/10 transition-colors">
                                            <td className="px-6 py-4">
                                                <span className="font-medium text-sm">{row.name}</span>
                                            </td>
                                            <td className="px-6 py-4">
                                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-md bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-mono text-xs font-medium">
                                                    {row.version}
                                                </span>
                                            </td>
                                            <td className="px-6 py-4 text-sm text-muted-foreground">
                                                {row.description}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    )}
                </div>
            )}
        </div>
    )
}