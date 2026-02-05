import { useState } from 'react'
import { Play, History, FileCode } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { REIFYDB_CONFIG } from '@/config'

export function QueryPage() {
  const [query, setQuery] = useState('')
  const [result, setResult] = useState<any>(null)
  const [isLoading, setIsLoading] = useState(false)

  const executeQuery = async () => {
    setIsLoading(true)
    try {
      const response = await fetch(`${REIFYDB_CONFIG.getApiUrl()}/v1/execute`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      })
      const data = await response.json()
      setResult(data)
    } catch (error) {
      setResult({ error: error instanceof Error ? error.message : 'Query failed' })
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Query Editor</h1>
          <p className="text-muted-foreground">Execute RQL queries on your database</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline">
            <History className="mr-2 h-4 w-4" />
            History
          </Button>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileCode className="h-4 w-4" />
              Query
            </CardTitle>
          </CardHeader>
          <CardContent>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Enter your RQL query here..."
              className="w-full h-64 p-4 font-mono text-sm bg-background border resize-none focus:outline-none focus:ring-2 focus:ring-ring"
            />
            <div className="flex gap-2 mt-4">
              <Button 
                onClick={executeQuery} 
                disabled={isLoading || !query.trim()}
                className="flex-1"
              >
                <Play className="mr-2 h-4 w-4" />
                {isLoading ? 'Executing...' : 'Execute'}
              </Button>
              <Button variant="outline" onClick={() => setQuery('')}>
                Clear
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Result</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64 overflow-auto">
              {result ? (
                <pre className="text-sm font-mono whitespace-pre-wrap">
                  {JSON.stringify(result, null, 2)}
                </pre>
              ) : (
                <p className="text-muted-foreground">Query results will appear here</p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent Queries</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="p-3 bg-muted font-mono text-sm">
              SELECT * FROM users WHERE active = true
            </div>
            <div className="p-3 bg-muted font-mono text-sm">
              INSERT INTO posts (title, content) VALUES ('Hello', 'World')
            </div>
            <div className="p-3 bg-muted font-mono text-sm">
              UPDATE users SET last_login = NOW() WHERE id = 1
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}