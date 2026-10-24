export const DEV_API_KEY = 'dev-api-key'

export async function postApi(method: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`/api/${method}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': DEV_API_KEY,
    },
    body: JSON.stringify(body),
  })
  const payload = await response.json() as Record<string, unknown>
  return { status: response.status, body: payload }
}

export function socketUrl(): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/connection/websocket`
}
