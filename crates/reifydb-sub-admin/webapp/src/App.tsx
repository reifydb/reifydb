import {useEffect, useState} from 'react'
import './App.css'

function App() {
    const [health, setHealth] = useState<any>(null)
    const [metrics, setMetrics] = useState<any>(null)
    const [query, setQuery] = useState('')
    const [queryResult, setQueryResult] = useState<any>(null)

    useEffect(() => {
        // Fetch health status
        fetch('/v1/health')
            .then(res => res.json())
            .then(data => setHealth(data))
            .catch(err => console.error('Health check failed:', err))

        // Fetch metrics
        fetch('/v1/metrics')
            .then(res => res.json())
            .then(data => setMetrics(data))
            .catch(err => console.error('Metrics fetch failed:', err))
    }, [])

    const executeQuery = async () => {
        try {
            const response = await fetch('/v1/execute', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({query}),
            })
            const result = await response.json()
            setQueryResult(result)
        } catch (err) {
            // @ts-ignore
            setQueryResult({error: err.toString()})
        }
    }

    return (
        <div className="app">
            <header className="app-header">
                <h1>ReifyDB Admin Console</h1>
            </header>

            <main className="app-main">
                <section className="status-section">
                    <h2>System Status</h2>
                    {health && (
                        <div className="status-card">
                            <h3>Health</h3>
                            <pre>{JSON.stringify(health, null, 2)}</pre>
                        </div>
                    )}
                    {metrics && (
                        <div className="status-card">
                            <h3>Metrics</h3>
                            <pre>{JSON.stringify(metrics, null, 2)}</pre>
                        </div>
                    )}
                </section>

                <section className="query-section">
                    <h2>Query Executor</h2>
                    <div className="query-input">
            <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Enter your RQL query here..."
                rows={5}
            />
                        <button onClick={executeQuery}>Execute</button>
                    </div>
                    {queryResult && (
                        <div className="query-result">
                            <h3>Result</h3>
                            <pre>{JSON.stringify(queryResult, null, 2)}</pre>
                        </div>
                    )}
                </section>
            </main>
        </div>
    )
}

export default App