import { Routes, Route } from 'react-router-dom'
import { Navbar } from './components/layout/navbar'
import { DashboardPage } from './pages/dashboard'
import { PipelinesPage } from './pages/pipelines'
import { PipelineDetailPage } from './pages/pipeline-detail'
import { RunDetailPage } from './pages/run-detail'
import { JobDetailPage } from './pages/job-detail'
import { SettingsPage } from './pages/settings'

export function App() {
  return (
    <div className="min-h-screen section-pattern">
      <Navbar />
      <main>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/pipelines" element={<PipelinesPage />} />
          <Route path="/pipelines/:id" element={<PipelineDetailPage />} />
          <Route path="/jobs/:id" element={<JobDetailPage />} />
          <Route path="/runs/:id" element={<RunDetailPage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Routes>
      </main>
    </div>
  )
}
