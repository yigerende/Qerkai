import type { ProxyProtocol } from '@/types'

// Parse proxy URL: protocol://user:pass@host:port or protocol://host:port
// Host may be a domain, IPv4, or bracketed IPv6 ([2001:db8::1]).
export const parseProxyUrl = (
  line: string
): {
  protocol: ProxyProtocol
  host: string
  port: number
  username: string
  password: string
} | null => {
  const trimmed = line.trim()
  if (!trimmed) return null

  // Provider shorthand: host:port:user:password defaults to HTTP.
  // Only the first three separators are structural; keep colons/@ in passwords.
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed)) {
    const parts = trimmed.match(/^(\[[0-9a-f:.]+\]|[^:\s/@\[\]]+):(\d+):([^:]+):(.+)$/i)
    if (!parts) return null
    const [, rawHost, port, username, password] = parts
    const portNum = Number(port)
    if (portNum < 1 || portNum > 65535 || !username.trim()) return null
    return {
      protocol: 'http',
      host: rawHost.replace(/^\[|\]$/g, ''),
      port: portNum,
      username: username.trim(),
      password
    }
  }

  // Regex to parse proxy URL (supports http, https, socks5, socks5h).
  // Host alternatives: [bracketed-IPv6] | hostname/IPv4 (colon-free, so the
  // match stops before the final :port).
  const regex =
    /^(https?|socks5h?):\/\/(?:([^:@\[\]]+):([^@\[\]]+)@)?(\[[0-9a-f:.]+\]|[^:\[\]]+):(\d+)$/i
  const match = trimmed.match(regex)

  if (!match) return null

  const [, protocol, username, password, rawHost, port] = match
  const portNum = parseInt(port, 10)

  if (portNum < 1 || portNum > 65535) return null

  // Strip brackets from IPv6 literals; the backend re-brackets via net.JoinHostPort.
  const host = rawHost.replace(/^\[|\]$/g, '').trim()

  return {
    protocol: protocol.toLowerCase() as ProxyProtocol,
    host,
    port: portNum,
    username: username?.trim() || '',
    password: password?.trim() || ''
  }
}

