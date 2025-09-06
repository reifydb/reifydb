import { Outlet } from '@tanstack/react-router'
import { Sidebar } from './sidebar.tsx'
import { Search, Bell, Sun, Moon, Command } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useState, useEffect } from 'react'
import { CommandPalette } from '@/components/command-palette'
import { useCommandPalette } from '@/hooks/use-command-palette'

export function MainLayout() {
  const [darkMode, setDarkMode] = useState(false)
  const commandPalette = useCommandPalette()

  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }, [darkMode])

  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="flex items-center justify-between px-6 py-4 border-b border-border bg-card">
          <div className="flex items-center gap-4 flex-1 max-w-xl">
            <Button
              variant="outline"
              className="relative flex-1 justify-start text-left font-normal text-muted-foreground"
              onClick={commandPalette.open}
            >
              <Search className="mr-2 h-4 w-4" />
              <span className="flex-1">Search or press</span>
              <kbd className="pointer-events-none ml-auto inline-flex h-5 select-none items-center gap-1 rounded border border-border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
                <Command className="h-3 w-3" />K
              </kbd>
            </Button>
          </div>
          
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon">
              <Bell className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setDarkMode(!darkMode)}
            >
              {darkMode ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </Button>
          </div>
        </header>

        <main className="flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
      
      <CommandPalette isOpen={commandPalette.isOpen} onClose={commandPalette.close} />
    </div>
  )
}