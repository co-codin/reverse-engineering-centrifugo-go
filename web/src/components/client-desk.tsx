import { useEffect, useRef, useState } from 'react'
import { Centrifuge, State } from 'centrifuge'
import type { SubscribedContext, Subscription } from 'centrifuge'
import { postApi, socketUrl } from '@/lib/api'
import { apiMessage, formatData, formatMsk, summarizeFrame } from '@/lib/format'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

interface DeskPublication {
  key: string
  channel: string
  data: unknown
  offset?: number
  user?: string
  at: number
}

interface DeskPresence {
  client: string
  user: string
}

interface Frame {
  id: number
  dir: 'in' | 'out'
  raw: string
  at: number
}

type LinkStatus = 'idle' | 'connecting' | 'connected' | 'disconnected'

function mergePublications(current: DeskPublication[], incoming: DeskPublication[]): DeskPublication[] {
  const byKey = new Map<string, DeskPublication>()
  for (const publication of [...incoming, ...current]) {
    if (!byKey.has(publication.key)) byKey.set(publication.key, publication)
  }
  return [...byKey.values()]
    .sort((left, right) => (left.offset ?? Number.MAX_SAFE_INTEGER) - (right.offset ?? Number.MAX_SAFE_INTEGER) || left.at - right.at)
    .slice(-80)
}

function protocolError(error: unknown): string | null {
  if (typeof error !== 'object' || error === null || !('code' in error) || !('message' in error)) return null
  return `${String(error.code)} ${String(error.message)}`
}

function createSocket(onFrame: (dir: 'in' | 'out', raw: string) => void) {
  return class LoggingSocket extends WebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      this.addEventListener('message', (event: MessageEvent) => {
        if (typeof event.data === 'string') onFrame('in', event.data)
      })
    }

    override send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
      if (typeof data === 'string') onFrame('out', data)
      super.send(data as string | Blob | ArrayBuffer)
    }
  }
}

export function ClientDesk({ id, title, defaultUser }: { id: string; title: string; defaultUser: string }) {
  const [user, setUser] = useState(defaultUser)
  const [status, setStatus] = useState<LinkStatus>('idle')
  const [detail, setDetail] = useState('')
  const [clientId, setClientId] = useState('')
  const [channel, setChannel] = useState('chat:lobby')
  const [subState, setSubState] = useState<'none' | 'subscribing' | 'subscribed'>('none')
  const [subDetail, setSubDetail] = useState('')
  const [recovery, setRecovery] = useState('')
  const [publications, setPublications] = useState<DeskPublication[]>([])
  const [presence, setPresence] = useState<DeskPresence[]>([])
  const [serverChannels, setServerChannels] = useState<string[]>([])
  const [draft, setDraft] = useState('')
  const [publishError, setPublishError] = useState('')
  const [frames, setFrames] = useState<Frame[]>([])
  const clientRef = useRef<Centrifuge | null>(null)
  const subRef = useRef<Subscription | null>(null)
  const mounted = useRef(true)
  const frameId = useRef(0)
  const sessionGen = useRef(0)
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
      clientRef.current?.disconnect()
      clientRef.current = null
    }
  }, [])

  useEffect(() => {
    const node = logRef.current
    if (node) node.scrollTop = node.scrollHeight
  }, [frames])

  function remember(channelName: string, data: unknown, offset: number | undefined, from: string | undefined, at: number) {
    const key = offset != null ? `${channelName}:${offset}` : `${channelName}:${at}:${formatData(data)}`
    setPublications((current) => mergePublications(current, [{ key, channel: channelName, data, offset, user: from, at }]))
  }

  async function refreshPresence(sub: Subscription) {
    try {
      const result = await sub.presence()
      if (!mounted.current || subRef.current !== sub) return
      setPresence(Object.values(result.clients).map((client) => ({
        client: client.client,
        user: client.user || 'anonymous',
      })))
    } catch {
      if (mounted.current && subRef.current === sub) setPresence([])
    }
  }

  async function onSubscribed(sub: Subscription, ctx: SubscribedContext) {
    if (!mounted.current || subRef.current !== sub) return
    setSubState('subscribed')
    setSubDetail('')
    if (ctx.wasRecovering && ctx.recovered) {
      setRecovery(ctx.hasRecoveredPublications
        ? 'Recovered missed publications from history.'
        : 'Caught up. Nothing was missed.')
    } else if (ctx.wasRecovering) {
      setRecovery('History no longer covers that position, so the stream jumped forward.')
    } else {
      setRecovery('')
      try {
        const history = await sub.history({ limit: 20 })
        if (!mounted.current || subRef.current !== sub) return
        setPublications((current) => mergePublications(current, history.publications.map((publication) => ({
          key: publication.offset != null ? `${sub.channel}:${publication.offset}` : `${sub.channel}:history:${formatData(publication.data)}`,
          channel: sub.channel,
          data: publication.data,
          offset: publication.offset,
          user: publication.info?.user,
          at: 0,
        }))))
      } catch (error) {
        if (!mounted.current) return
        setSubDetail(protocolError(error) ?? 'History request failed.')
      }
    }
    await refreshPresence(sub)
  }

  async function openSession() {
    const generation = sessionGen.current + 1
    sessionGen.current = generation
    setStatus('connecting')
    setDetail('')
    setPublications([])
    setPresence([])
    setServerChannels([])
    setFrames([])
    setRecovery('')
    setSubState('none')
    const name = user.trim()
    let token = ''
    if (name) {
      try {
        const { body } = await postApi('token', { user: name, info: { name } })
        if (body.error) throw new Error(apiMessage(body))
        token = (body.result as { token: string }).token
      } catch (error) {
        if (generation !== sessionGen.current) return
        setStatus('disconnected')
        setDetail(error instanceof Error ? error.message : 'Token request failed.')
        return
      }
    }
    if (!mounted.current || generation !== sessionGen.current) return
    clientRef.current?.disconnect()

    const centrifuge = new Centrifuge(socketUrl(), {
      websocket: createSocket((dir, raw) => {
        if (!mounted.current) return
        frameId.current += 1
        const id = frameId.current
        setFrames((current) => [...current, { id, dir, raw, at: Date.now() }].slice(-30))
      }),
      token,
      name: 'console',
      version: '0.1.0',
    })
    clientRef.current = centrifuge
    centrifuge.on('connected', (ctx) => {
      setStatus('connected')
      setClientId(ctx.client)
      setDetail('')
    })
    centrifuge.on('disconnected', (ctx) => {
      setStatus('disconnected')
      setClientId('')
      setSubState('none')
      subRef.current = null
      if (ctx.reason && ctx.reason !== 'disconnect called') setDetail(`${ctx.code} ${ctx.reason}`)
    })
    centrifuge.on('error', (ctx) => setDetail(ctx.error.message))
    centrifuge.on('subscribed', (ctx) => {
      setServerChannels((current) => current.includes(ctx.channel) ? current : [...current, ctx.channel])
    })
    centrifuge.on('unsubscribed', (ctx) => {
      setServerChannels((current) => current.filter((item) => item !== ctx.channel))
    })
    centrifuge.on('publication', (ctx) => remember(ctx.channel, ctx.data, ctx.offset, ctx.info?.user, Date.now()))
    centrifuge.connect()
  }

  function closeSession() {
    sessionGen.current += 1
    clientRef.current?.disconnect()
    clientRef.current = null
    subRef.current = null
    setStatus('disconnected')
    setSubState('none')
    setClientId('')
    setPresence([])
    setServerChannels([])
  }

  function joinChannel() {
    const centrifuge = clientRef.current
    if (!centrifuge || centrifuge.state !== State.Connected) return
    const name = channel.trim()
    if (!name) return
    const previous = subRef.current
    if (previous) {
      previous.unsubscribe()
      centrifuge.removeSubscription(previous)
      subRef.current = null
    }
    setPublications([])
    setPresence([])
    setRecovery('')
    setSubDetail('')
    setSubState('subscribing')
    const sub = centrifuge.newSubscription(name)
    subRef.current = sub
    sub.on('publication', (ctx) => remember(name, ctx.data, ctx.offset, ctx.info?.user, Date.now()))
    sub.on('subscribed', (ctx) => { void onSubscribed(sub, ctx) })
    sub.on('join', () => { void refreshPresence(sub) })
    sub.on('leave', () => { void refreshPresence(sub) })
    sub.on('unsubscribed', (ctx) => {
      if (subRef.current !== sub) return
      setSubState('none')
      setPresence([])
      if (ctx.code >= 100) setSubDetail(`${ctx.code} ${ctx.reason}`.trim())
    })
    sub.on('error', (ctx) => setSubDetail(ctx.error.message))
    sub.subscribe()
  }

  function leaveChannel() {
    const centrifuge = clientRef.current
    const sub = subRef.current
    if (!centrifuge || !sub) return
    sub.unsubscribe()
    centrifuge.removeSubscription(sub)
    subRef.current = null
    setSubState('none')
    setPresence([])
    setRecovery('')
  }

  async function sendPublication() {
    const sub = subRef.current
    const text = draft.trim()
    if (!sub || !text) return
    setPublishError('')
    try {
      await sub.publish({ text, from: user.trim() || 'anonymous' })
      setDraft('')
    } catch (error) {
      setPublishError(protocolError(error) ?? 'Publish failed.')
    }
  }

  const connected = status === 'connected'
  const statusLabel = status === 'idle' ? 'idle' : status

  return (
    <section className="flex flex-col gap-4 rounded-lg border bg-card p-4 shadow-xs">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="font-serif text-2xl tracking-tight">{title}</h2>
          <p className="mt-1 font-mono text-[11px] text-muted-foreground">
            {clientId ? clientId : 'no session'}
          </p>
        </div>
        <Badge variant={connected ? 'default' : 'secondary'}>{statusLabel}</Badge>
      </div>

      <div className="grid gap-2 sm:grid-cols-[1fr_auto]">
        <div className="grid gap-1">
          <label htmlFor={`${id}-user`} className="text-xs text-muted-foreground">User</label>
          <Input
            id={`${id}-user`}
            value={user}
            onChange={(event) => setUser(event.target.value)}
            placeholder="blank connects anonymously"
            autoComplete="off"
            disabled={connected || status === 'connecting'}
          />
        </div>
        <div className="flex items-end">
          {connected || status === 'connecting'
            ? <Button type="button" variant="outline" onClick={closeSession}>Disconnect</Button>
            : <Button type="button" onClick={() => { void openSession() }}>Connect</Button>}
        </div>
      </div>
      {detail ? <p className="text-sm text-destructive" role="status">{detail}</p> : null}

      <div className="grid gap-2 sm:grid-cols-[1fr_auto]">
        <div className="grid gap-1">
          <label htmlFor={`${id}-channel`} className="text-xs text-muted-foreground">Channel</label>
          <Input
            id={`${id}-channel`}
            value={channel}
            onChange={(event) => setChannel(event.target.value)}
            spellCheck={false}
            disabled={!connected}
          />
        </div>
        <div className="flex items-end gap-2">
          {subState === 'subscribed'
            ? <Button type="button" variant="outline" onClick={leaveChannel}>Leave</Button>
            : <Button type="button" onClick={joinChannel} disabled={!connected || subState === 'subscribing'}>{subState === 'subscribing' ? 'Joining...' : 'Subscribe'}</Button>}
        </div>
      </div>
      {subDetail ? <p className="text-sm text-destructive" role="status">{subDetail}</p> : null}
      {recovery ? <p className="text-sm text-[#1f3d32]">{recovery}</p> : null}
      {serverChannels.length > 0 ? (
        <p className="text-sm text-muted-foreground">Server subscriptions: {serverChannels.join(', ')}</p>
      ) : null}

      <div className="grid gap-2">
        <label htmlFor={`${id}-draft`} className="text-xs text-muted-foreground">Publication</label>
        <div className="flex gap-2">
          <Input
            id={`${id}-draft`}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault()
                void sendPublication()
              }
            }}
            placeholder={subState === 'subscribed' ? 'Message the channel' : 'Subscribe before sending'}
            disabled={subState !== 'subscribed'}
          />
          <Button type="button" onClick={() => { void sendPublication() }} disabled={subState !== 'subscribed' || draft.trim() === ''}>Send</Button>
        </div>
        {publishError ? <p className="text-sm text-destructive" role="status">{publishError}</p> : null}
      </div>

      <div>
        <div className="mb-2 flex items-baseline justify-between gap-3">
          <h3 className="text-sm font-medium">Publications</h3>
          <p className="text-[11px] text-muted-foreground">times in Moscow</p>
        </div>
        {publications.length === 0 ? (
          <p className="rounded-md border border-dashed px-3 py-6 text-sm text-muted-foreground">
            {status === 'idle' && 'This client is idle. Connect it to open a session.'}
            {status === 'connecting' && 'Opening the WebSocket session...'}
            {status === 'disconnected' && 'Disconnected. Connect again to open a new session.'}
            {status === 'connected' && subState !== 'subscribed' && 'Connected. Subscribe to a channel to receive publications.'}
            {status === 'connected' && subState === 'subscribed' && 'No publications yet. Send one here, or from the application API below.'}
          </p>
        ) : (
          <ol className="max-h-64 space-y-2 overflow-auto pr-1">
            {publications.map((publication) => (
              <li key={publication.key} className="rounded-md border bg-background px-3 py-2">
                <div className="flex items-baseline justify-between gap-3 font-mono text-[11px] text-muted-foreground">
                  <span>{publication.channel}{publication.offset != null ? ` · #${publication.offset}` : ''}</span>
                  <span>{publication.at > 0 ? `${formatMsk(publication.at)} MSK` : 'history'}</span>
                </div>
                <p className="mt-1 break-words font-mono text-sm">{formatData(publication.data)}</p>
                {publication.user !== undefined ? <p className="mt-1 text-xs text-muted-foreground">from {publication.user || 'anonymous'}</p> : null}
              </li>
            ))}
          </ol>
        )}
      </div>

      <div>
        <h3 className="mb-2 text-sm font-medium">Presence</h3>
        {presence.length === 0 ? (
          <p className="text-sm text-muted-foreground">Nobody is listed yet.</p>
        ) : (
          <ul className="flex flex-wrap gap-2">
            {presence.map((member) => (
              <li key={member.client}>
                <Badge variant="outline">{member.user} · {member.client.slice(0, 8)}</Badge>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div>
        <h3 className="mb-2 text-sm font-medium">Wire</h3>
        <div ref={logRef} className="h-44 overflow-auto rounded-md bg-[#221f1b] p-3 font-mono text-[11px] leading-5 text-[#efe7d6]">
          {frames.length === 0 ? (
            <p className="text-[#b7ad9d]">Frames appear here after you connect.</p>
          ) : frames.map((frame) => (
            <p key={frame.id} className="mb-1 break-all" title={frame.raw}>
              <span className={frame.dir === 'in' ? 'text-[#9ec7e8]' : 'text-[#e2b07a]'}>{frame.dir}</span>
              {' '}
              {summarizeFrame(frame.raw, frame.dir)}
              <span className="text-[#8d8476]"> · {formatMsk(frame.at)}</span>
            </p>
          ))}
        </div>
      </div>
    </section>
  )
}
