import { useEffect, useState } from 'react'
import { Activity, Server, Database, Users } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { REIFYDB_CONFIG } from '@/config'

export function DashboardPage() {
  const [health, setHealth] = useState<any>(null)
  const [metrics, setMetrics] = useState<any>(null)

  useEffect(() => {
    const apiUrl = REIFYDB_CONFIG.getApiUrl()

    // Fetch health status
    fetch(`${apiUrl}/v1/health`)
      .then(res => res.json())
      .then(data => setHealth(data))
      .catch(err => console.error('Health check failed:', err))

    // Fetch metrics
    fetch(`${apiUrl}/v1/metrics`)
      .then(res => res.json())
      .then(data => setMetrics(data))
      .catch(err => console.error('Metrics fetch failed:', err))
  }, [])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="text-muted-foreground">Overview of your ReifyDB instance</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">System Status</CardTitle>
            <Server className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {health ? 'Healthy' : 'Checking...'}
            </div>
            <p className="text-xs text-muted-foreground">All systems operational</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Connections</CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">23</div>
            <p className="text-xs text-muted-foreground">+12% from last hour</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Database Size</CardTitle>
            <Database className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">1.2 GB</div>
            <p className="text-xs text-muted-foreground">Across 12 tables</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Queries/sec</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">142</div>
            <p className="text-xs text-muted-foreground">5 min average</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Health Status</CardTitle>
          </CardHeader>
          <CardContent>
            {health ? (
              <pre className="text-sm bg-muted p-4 overflow-auto">
                {JSON.stringify(health, null, 2)}
              </pre>
            ) : (
              <p className="text-muted-foreground">Loading health data...</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>System Metrics</CardTitle>
          </CardHeader>
          <CardContent>
            {metrics ? (
              <pre className="text-sm bg-muted p-4 overflow-auto">
                {JSON.stringify(metrics, null, 2)}
              </pre>
            ) : (
              <p className="text-muted-foreground">Loading metrics data...</p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}