import { useState } from 'react'
import { postApi } from '@/lib/api'
import { apiMessage } from '@/lib/format'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'

const starter = '{\n  "text": "hello from the application"\n}'

export function BackendDesk() {
  const [channel, setChannel] = useState('chat:lobby')
  const [payload, setPayload] = useState(starter)
  const [user, setUser] = useState('alice')
  const [serverChannel, setServerChannel] = useState('news:desk')
  const [pending, setPending] = useState('')
  const [response, setResponse] = useState('')
  const [error, setError] = useState('')

  async function run(label: string, method: string, body: unknown) {
    setPending(label)
    setError('')
    try {
      const result = await postApi(method, body)
      if (result.body.error) {
        setError(apiMessage(result.body))
        setResponse(JSON.stringify(result.body, null, 2))
        return
      }
      setResponse(JSON.stringify(result.body, null, 2))
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Request failed.')
      setResponse('')
    } finally {
      setPending('')
    }
  }

  function publish() {
    let data: unknown
    try {
      data = JSON.parse(payload) as unknown
    } catch {
      setError('The payload is not valid JSON.')
      setResponse('')
      return
    }
    void run('Publishing\u2026', 'publish', { channel: channel.trim(), data })
  }

  return (
    <section className="mt-4 rounded-lg border bg-card p-4 shadow-xs">
      <div className="max-w-2xl">
        <h2 className="font-serif text-2xl tracking-tight">Application API</h2>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">
          This is the call your backend makes. <span className="font-mono">chat</span> lets a subscribed client publish.
          <span className="font-mono"> news</span> accepts publications only from this API. The dev key is <span className="font-mono">dev-api-key</span>.
        </p>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_18rem]">
        <div className="grid gap-3">
          <div className="grid gap-1">
            <label htmlFor="api-channel" className="text-xs text-muted-foreground">Channel</label>
            <Input id="api-channel" value={channel} onChange={(event) => setChannel(event.target.value)} spellCheck={false} />
          </div>
          <div className="grid gap-1">
            <label htmlFor="api-payload" className="text-xs text-muted-foreground">JSON data</label>
            <Textarea id="api-payload" value={payload} onChange={(event) => setPayload(event.target.value)} spellCheck={false} />
          </div>
          <div className="flex flex-wrap gap-2">
            <Button type="button" onClick={publish} disabled={pending !== '' || channel.trim() === ''}>
              {pending === 'Publishing\u2026' ? pending : 'Publish'}
            </Button>
            <Button
              type="button"
              variant="outline"
              disabled={pending !== ''}
              onClick={() => {
                setChannel('news:desk')
                setPayload('{\n  "headline": "desk update"\n}')
                void run('Publishing\u2026', 'publish', { channel: 'news:desk', data: { headline: 'desk update' } })
              }}
            >
              Publish to news:desk
            </Button>
          </div>
        </div>

        <div className="grid content-start gap-3">
          <div className="grid gap-1">
            <label htmlFor="api-user" className="text-xs text-muted-foreground">User id</label>
            <Input id="api-user" value={user} onChange={(event) => setUser(event.target.value)} autoComplete="off" />
          </div>
          <div className="grid gap-1">
            <label htmlFor="api-server-channel" className="text-xs text-muted-foreground">Server subscription</label>
            <Input id="api-server-channel" value={serverChannel} onChange={(event) => setServerChannel(event.target.value)} spellCheck={false} />
          </div>
          <Button
            type="button"
            variant="outline"
            disabled={pending !== '' || user.trim() === '' || serverChannel.trim() === ''}
            onClick={() => { void run('Subscribing\u2026', 'subscribe', { user: user.trim(), channel: serverChannel.trim() }) }}
          >
            {pending === 'Subscribing\u2026' ? pending : 'Subscribe user'}
          </Button>
          <Button
            type="button"
            variant="destructive"
            disabled={pending !== '' || user.trim() === ''}
            onClick={() => { void run('Disconnecting\u2026', 'disconnect', { user: user.trim() }) }}
          >
            {pending === 'Disconnecting\u2026' ? pending : 'Disconnect user'}
          </Button>
        </div>
      </div>

      <div className="mt-4">
        <h3 className="mb-2 text-sm font-medium">Response</h3>
        {error ? <p className="mb-2 text-sm text-destructive" role="status">{error}</p> : null}
        {response ? (
          <pre className="overflow-auto rounded-md bg-[#221f1b] p-3 font-mono text-xs leading-5 text-[#efe7d6]">{response}</pre>
        ) : (
          <p className="rounded-md border border-dashed px-3 py-6 text-sm text-muted-foreground">
            {pending ? pending : 'The API response shows up here.'}
          </p>
        )}
      </div>
    </section>
  )
}
