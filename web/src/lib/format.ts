export function formatMsk(at: number): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Moscow',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).format(at)
}

export function formatData(data: unknown): string {
  if (typeof data === 'string') return data
  try {
    return JSON.stringify(data)
  } catch {
    return String(data)
  }
}

export function summarizeFrame(raw: string, dir: 'in' | 'out'): string {
  const lines = raw.split('\n').filter((line) => line.trim() !== '')
  if (lines.length === 0) return 'empty'
  return lines.map((line) => {
    try {
      const message = JSON.parse(line) as Record<string, unknown>
      if (Object.keys(message).length === 0) return dir === 'in' ? 'ping' : 'pong'
      const push = message.push
      if (isRecord(push)) {
        const kind = ['pub', 'join', 'leave', 'subscribe', 'unsubscribe', 'disconnect', 'message'].find((key) => key in push)
        const channel = typeof push.channel === 'string' ? ` ${push.channel}` : ''
        return `push ${kind ?? 'event'}${channel}`
      }
      if (isRecord(message.error)) return `error ${String(message.error.code ?? '')}`
      const kind = ['connect', 'subscribe', 'unsubscribe', 'publish', 'presence', 'presence_stats', 'history', 'rpc'].find((key) => key in message)
      if (kind) return message.id ? `${kind} #${String(message.id)}` : kind
      return 'frame'
    } catch {
      return 'frame'
    }
  }).join(' · ')
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

export function apiMessage(body: Record<string, unknown>): string {
  if (!isRecord(body.error)) return 'request failed'
  const code = body.error.code
  const message = body.error.message
  return `${String(code ?? '')} ${String(message ?? 'request failed')}`.trim()
}
