import React, { useState, useEffect, useRef, useMemo } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { 
  Search, 
  Database, 
  Users, 
  Activity, 
  Settings,
  FileText,
  Terminal,
  X,
  ArrowRight,
  Command,
  Info
} from 'lucide-react'

interface CommandItem {
  id: string
  title: string
  description?: string
  icon: React.ElementType
  action: () => void
  category: 'navigation' | 'action' | 'recent'
  keywords?: string[]
}

interface CommandPaletteProps {
  isOpen: boolean
  onClose: () => void
}

export function CommandPalette({ isOpen, onClose }: CommandPaletteProps) {
  const [search, setSearch] = useState('')
  const [selectedIndex, setSelectedIndex] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const navigate = useNavigate()

  // Define all available commands
  const commands: CommandItem[] = useMemo(() => [
    // Navigation commands
    {
      id: 'nav-schema',
      title: 'Go to Schema',
      description: 'View and manage database tables',
      icon: Database,
      action: () => {
        navigate({ to: '/schema' })
        onClose()
      },
      category: 'navigation',
      keywords: ['database', 'tables', 'schema']
    },
    {
      id: 'nav-query',
      title: 'Go to Query Editor',
      description: 'Execute SQL queries',
      icon: Terminal,
      action: () => {
        navigate({ to: '/query' })
        onClose()
      },
      category: 'navigation',
      keywords: ['sql', 'query', 'execute']
    },
    {
      id: 'nav-users',
      title: 'Go to Users',
      description: 'Manage database users',
      icon: Users,
      action: () => {
        navigate({ to: '/users' })
        onClose()
      },
      category: 'navigation',
      keywords: ['users', 'permissions', 'access']
    },
    {
      id: 'nav-monitoring',
      title: 'Go to Monitoring',
      description: 'View system metrics',
      icon: Activity,
      action: () => {
        navigate({ to: '/monitoring' })
        onClose()
      },
      category: 'navigation',
      keywords: ['metrics', 'performance', 'monitoring']
    },
    {
      id: 'nav-settings',
      title: 'Go to Settings',
      description: 'Configure system settings',
      icon: Settings,
      action: () => {
        navigate({ to: '/settings' })
        onClose()
      },
      category: 'navigation',
      keywords: ['settings', 'configuration', 'preferences']
    },
    {
      id: 'nav-version',
      title: 'Go to Version',
      description: 'View system version information',
      icon: Info,
      action: () => {
        navigate({ to: '/version' })
        onClose()
      },
      category: 'navigation',
      keywords: ['version', 'info', 'about', 'system']
    },
    // Action commands
    {
      id: 'action-new-table',
      title: 'Create New Table',
      description: 'Create a new database table',
      icon: Database,
      action: () => {
        navigate({ to: '/schema' })
        // TODO: Open new table dialog
        onClose()
      },
      category: 'action',
      keywords: ['create', 'new', 'table']
    },
    {
      id: 'action-new-query',
      title: 'New Query',
      description: 'Open a new query editor',
      icon: Terminal,
      action: () => {
        navigate({ to: '/query' })
        // TODO: Open new query tab
        onClose()
      },
      category: 'action',
      keywords: ['new', 'query', 'sql']
    },
    {
      id: 'action-docs',
      title: 'View Documentation',
      description: 'Open ReifyDB documentation',
      icon: FileText,
      action: () => {
        window.open('https://docs.reifydb.com', '_blank')
        onClose()
      },
      category: 'action',
      keywords: ['docs', 'documentation', 'help']
    }
  ], [navigate, onClose])

  // Filter commands based on search
  const filteredCommands = useMemo(() => {
    if (!search) return commands

    const searchLower = search.toLowerCase()
    return commands.filter(cmd => {
      const titleMatch = cmd.title.toLowerCase().includes(searchLower)
      const descMatch = cmd.description?.toLowerCase().includes(searchLower)
      const keywordMatch = cmd.keywords?.some(k => k.toLowerCase().includes(searchLower))
      return titleMatch || descMatch || keywordMatch
    })
  }, [search, commands])

  // Group commands by category
  const groupedCommands = useMemo(() => {
    const groups: Record<string, CommandItem[]> = {
      action: [],
      navigation: [],
      recent: []
    }
    
    filteredCommands.forEach(cmd => {
      groups[cmd.category].push(cmd)
    })
    
    return groups
  }, [filteredCommands])

  // Handle keyboard navigation
  useEffect(() => {
    if (!isOpen) return

    const handleKeyDown = (e: KeyboardEvent) => {
      const totalCommands = filteredCommands.length

      switch (e.key) {
        case 'ArrowDown':
          e.preventDefault()
          setSelectedIndex((prev) => (prev + 1) % totalCommands)
          break
        case 'ArrowUp':
          e.preventDefault()
          setSelectedIndex((prev) => (prev - 1 + totalCommands) % totalCommands)
          break
        case 'Enter':
          e.preventDefault()
          if (filteredCommands[selectedIndex]) {
            filteredCommands[selectedIndex].action()
          }
          break
        case 'Escape':
          e.preventDefault()
          onClose()
          break
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [isOpen, filteredCommands, selectedIndex, onClose])

  // Reset and focus when opened
  useEffect(() => {
    if (isOpen) {
      setSearch('')
      setSelectedIndex(0)
      setTimeout(() => inputRef.current?.focus(), 0)
    }
  }, [isOpen])

  if (!isOpen) return null

  let commandIndex = 0

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-[20vh]">
      {/* Backdrop */}
      <div 
        className="absolute inset-0 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />
      
      {/* Command Palette */}
      <div className="relative w-full max-w-2xl bg-background border border-border shadow-2xl">
        {/* Search Input */}
        <div className="flex items-center border-b border-border">
          <Search className="ml-4 h-5 w-5 text-muted-foreground" />
          <input
            ref={inputRef}
            type="text"
            className="flex-1 bg-transparent px-4 py-4 text-sm outline-none placeholder:text-muted-foreground"
            placeholder="Type a command or search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <button
            onClick={onClose}
            className="mr-4 p-1 hover:bg-accent rounded"
          >
            <X className="h-4 w-4 text-muted-foreground" />
          </button>
        </div>

        {/* Command List */}
        <div className="max-h-[400px] overflow-y-auto py-2">
          {filteredCommands.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-muted-foreground">
              No commands found for "{search}"
            </div>
          ) : (
            <>
              {/* Actions */}
              {groupedCommands.action.length > 0 && (
                <div>
                  <div className="px-4 py-2 text-xs font-medium text-muted-foreground">
                    ACTIONS
                  </div>
                  {groupedCommands.action.map((cmd) => {
                    const isSelected = commandIndex === selectedIndex
                    const currentIndex = commandIndex++
                    const Icon = cmd.icon
                    
                    return (
                      <button
                        key={cmd.id}
                        className={`w-full flex items-center gap-3 px-4 py-2 text-left transition-colors ${
                          isSelected ? 'bg-accent' : 'hover:bg-accent/50'
                        }`}
                        onClick={cmd.action}
                        onMouseEnter={() => setSelectedIndex(currentIndex)}
                      >
                        <Icon className="h-4 w-4 text-muted-foreground shrink-0" />
                        <div className="flex-1 min-w-0">
                          <div className="text-sm font-medium">{cmd.title}</div>
                          {cmd.description && (
                            <div className="text-xs text-muted-foreground truncate">
                              {cmd.description}
                            </div>
                          )}
                        </div>
                        {isSelected && (
                          <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0" />
                        )}
                      </button>
                    )
                  })}
                </div>
              )}

              {/* Navigation */}
              {groupedCommands.navigation.length > 0 && (
                <div>
                  <div className="px-4 py-2 text-xs font-medium text-muted-foreground">
                    NAVIGATION
                  </div>
                  {groupedCommands.navigation.map((cmd) => {
                    const isSelected = commandIndex === selectedIndex
                    const currentIndex = commandIndex++
                    const Icon = cmd.icon
                    
                    return (
                      <button
                        key={cmd.id}
                        className={`w-full flex items-center gap-3 px-4 py-2 text-left transition-colors ${
                          isSelected ? 'bg-accent' : 'hover:bg-accent/50'
                        }`}
                        onClick={cmd.action}
                        onMouseEnter={() => setSelectedIndex(currentIndex)}
                      >
                        <Icon className="h-4 w-4 text-muted-foreground shrink-0" />
                        <div className="flex-1 min-w-0">
                          <div className="text-sm font-medium">{cmd.title}</div>
                          {cmd.description && (
                            <div className="text-xs text-muted-foreground truncate">
                              {cmd.description}
                            </div>
                          )}
                        </div>
                        {isSelected && (
                          <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0" />
                        )}
                      </button>
                    )
                  })}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="border-t border-border px-4 py-2 flex items-center justify-between text-xs text-muted-foreground">
          <div className="flex items-center gap-4">
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-muted font-mono">↑↓</kbd>
              Navigate
            </span>
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-muted font-mono">↵</kbd>
              Select
            </span>
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-muted font-mono">esc</kbd>
              Close
            </span>
          </div>
          <div className="flex items-center gap-1">
            <Command className="h-3 w-3" />
            <kbd className="font-mono">K</kbd>
          </div>
        </div>
      </div>
    </div>
  )
}