// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export function SettingsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
        <p className="text-muted-foreground">Configure your ReifyDB instance</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>System Settings</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">Settings configuration coming soon...</p>
        </CardContent>
      </Card>
    </div>
  )
}