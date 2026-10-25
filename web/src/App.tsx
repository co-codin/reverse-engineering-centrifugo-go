import { useEffect, useState } from 'react'
import { BackendDesk } from '@/components/backend-desk'
import { ClientDesk } from '@/components/client-desk'
import { postApi } from '@/lib/api'

interface NodeInfo {
  num_clients: number
  num_users: number
  num_channels: number
  uptime: number
  version: string
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  return `${minutes}m ${seconds % 60}s`
}

export default function App() {
  const [node, setNode] = useState<NodeInfo | null>(null)
  const [offline, setOffline] = useState(false)

  useEffect(() => {
    let stop = false
    async function tick() {
      try {
        const { status, body } = await postApi('info', {})
        if (stop) return
        const info = (body.result as { nodes?: NodeInfo[] } | undefined)?.nodes?.[0]
        if (status !== 200 || !info) {
          setOffline(true)
          return
        }
        setNode(info)
        setOffline(false)
      } catch {
        if (!stop) setOffline(true)
      }
    }
    void tick()
    const timer = window.setInterval(() => { void tick() }, 2000)
    return () => {
      stop = true
      window.clearInterval(timer)
    }
  }, [])

  return (
    <main className="mx-auto max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <header className="flex flex-col gap-6 border-b border-border pb-6 sm:flex-row sm:items-end sm:justify-between">
        <div className="max-w-2xl">
          <p className="font-mono text-[11px] tracking-[0.16em] text-muted-foreground uppercase">channeld</p>
          <h1 className="mt-1 font-serif text-3xl tracking-tight sm:text-4xl">A realtime node of your own</h1>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            The realtime node is a Go process, the same language as Centrifugo. These panels are two official centrifuge-js sessions. The form below publishes the way an application backend would.
          </p>
        </div>
        <dl className="grid grid-cols-3 gap-4 text-sm">
          <div>
            <dt className="text-xs text-muted-foreground">Clients</dt>
            <dd className="font-mono text-lg">{offline ? '—' : node?.num_clients ?? '…'}</dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Channels</dt>
            <dd className="font-mono text-lg">{offline ? '—' : node?.num_channels ?? '…'}</dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Uptime</dt>
            <dd className="font-mono text-lg">{offline ? 'down' : node ? formatUptime(node.uptime) : '…'}</dd>
          </div>
        </dl>
      </header>
      {offline ? (
        <p className="mt-4 rounded-md border border-destructive/40 bg-card px-3 py-2 text-sm text-destructive" role="status">
          The node is not answering <span className="font-mono">/api/info</span>. Start it with <span className="font-mono">npm run dev</span>.
        </p>
      ) : null}

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <ClientDesk id="a" title="Client A" defaultUser="alice" />
        <ClientDesk id="b" title="Client B" defaultUser="bob" />
      </div>
      <BackendDesk />

      <footer className="mt-8 max-w-3xl text-xs leading-5 text-muted-foreground">
        <p>
          JSON WebSocket at <span className="font-mono">/connection/websocket</span>. HTTP API at <span className="font-mono">/api/publish</span>.
          History stays in memory for five minutes, so a reconnect can recover what it missed. Clocks in the console are Moscow time.
          Version {node?.version ?? '0.1.0'}.
        </p>
      </footer>
    </main>
  )
}
