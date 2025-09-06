import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export function ApiDocsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">API Documentation</h1>
        <p className="text-muted-foreground">ReifyDB API reference and examples</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>API Documentation</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">API documentation coming soon...</p>
        </CardContent>
      </Card>
    </div>
  )
}