import { useRef, useEffect, useReducer } from 'react'
import { Shell, WsExecutor, COLORS } from '@reifydb/shell'
import { useConnection } from '@reifydb/react'
import { Maximize2, Minimize2, Terminal, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import '@xterm/xterm/css/xterm.css'

export function ShellPage() {
  const containerRef = useRef<HTMLDivElement>(null)
  const shellRef = useRef<Shell | null>(null)
  const { client, isConnected, isConnecting } = useConnection()
  const [, forceUpdate] = useReducer(x => x + 1, 0)

  useEffect(() => {
    if (!containerRef.current || !client || !isConnected) return

    const shell = new Shell(containerRef.current, {
      executor: new WsExecutor(client),
      welcomeMessage: [
        '',
        `${COLORS.bold}${COLORS.cyan}ReifyDB Admin Shell${COLORS.reset}`,
        '',
        `Type ${COLORS.green}.help${COLORS.reset} for available commands`,
        `Statements must end with a semicolon ${COLORS.yellow};${COLORS.reset}`,
        '',
      ],
      onFullscreenChange: () => forceUpdate(),
    })
    shell.start()
    shellRef.current = shell

    return () => {
      shell.dispose()
      shellRef.current = null
    }
  }, [client, isConnected])


  // Show loading state while connecting
  if (isConnecting || !isConnected) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-semibold">Shell</h1>
          <p className="text-muted-foreground">Interactive RQL terminal</p>
        </div>
        <div className="flex items-center justify-center h-[calc(100vh-200px)] bg-[#1e1e2e] rounded-lg">
          <div className="flex items-center gap-3 text-white/70">
            <Loader2 className="h-5 w-5 animate-spin" />
            <span>Connecting to database...</span>
          </div>
        </div>
      </div>
    )
  }

  const isFullscreen = shellRef.current?.isFullscreen ?? false

  const toggleFullscreen = () => {
    if (shellRef.current?.isFullscreen) {
      shellRef.current.exitFullscreen()
    } else {
      shellRef.current?.enterFullscreen()
    }
    forceUpdate()
  }

  return (
    <div className={cn(
      isFullscreen && "fixed inset-0 z-50 bg-[#1e1e2e] p-[3px]",
      !isFullscreen && "space-y-6"
    )}>
      {/* Header - only shown when not fullscreen */}
      {!isFullscreen && (
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold flex items-center gap-2">
              <Terminal className="h-6 w-6" />
              Shell
            </h1>
            <p className="text-muted-foreground">Interactive RQL terminal</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={toggleFullscreen}
          >
            <Maximize2 className="h-4 w-4 mr-2" />
            Fullscreen
          </Button>
        </div>
      )}

      {/* Minimize button - overlaid in fullscreen */}
      {isFullscreen && (
        <Button
          variant="ghost"
          size="icon"
          onClick={toggleFullscreen}
          className="absolute top-[3px] right-[3px] z-10 text-white/50 hover:text-white hover:bg-white/10"
        >
          <Minimize2 className="h-4 w-4" />
        </Button>
      )}

      {/* Terminal container */}
      <div
        ref={containerRef}
        className={cn(
          "bg-[#1e1e2e]",
          isFullscreen && "w-full h-full",
          !isFullscreen && "h-[calc(100vh-200px)] rounded-lg overflow-hidden"
        )}
      />
    </div>
  )
}
