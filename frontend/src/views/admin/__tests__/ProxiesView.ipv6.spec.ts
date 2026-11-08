import { describe, expect, it } from 'vitest'
import { parseProxyUrl } from '@/utils/proxyInput'

describe('proxy quick add parsing', () => {
  it.each([
    'socks5://[2001:db8::1]:1080',
    'socks5h://[2001:db8::1]:1080',
    'http://[::1]:8080',
    'socks5://user:pass@[2001:db8::1]:1080',
    'socks5://proxy.example.com:1080',
    'http://192.168.1.1:8080',
    'socks5://user:pass@proxy.example.com:1080',
    'https://user:pass@proxy.example.com:443'
  ])('preserves URL support: %s', line => {
    expect(parseProxyUrl(line)).not.toBeNull()
  })

  it('extracts bracketed IPv6 and its credentials', () => {
    expect(parseProxyUrl('socks5://user:pass@[2001:db8::1]:1080')).toEqual({
      protocol: 'socks5', host: '2001:db8::1', port: 1080, username: 'user', password: 'pass'
    })
  })

  it('defaults provider shorthand to HTTP and produces the same import fields as a URL', () => {
    expect(parseProxyUrl(' 192.0.2.1:12323:example-user:example-pass ')).toEqual(
      parseProxyUrl('http://example-user:example-pass@192.0.2.1:12323')
    )
    expect(parseProxyUrl('192.0.2.1:12323:example-user:example-pass')?.protocol).toBe('http')
  })

  it('keeps colons, at-signs, and percent signs in shorthand passwords', () => {
    expect(parseProxyUrl('proxy.example.com:8080:user:pass:with@percent%')).toEqual({
      protocol: 'http', host: 'proxy.example.com', port: 8080, username: 'user', password: 'pass:with@percent%'
    })
  })

  it('accepts bracketed IPv6 shorthand', () => {
    expect(parseProxyUrl('[2001:db8::1]:8080:user:pass')?.host).toBe('2001:db8::1')
  })

  it.each([
    '', 'socks5://2001:db8::1:1080', 'ftp://example.com:21', 'socks5://example.com:port',
    'host:0:user:pass', 'host:65536:user:pass', 'host:8080::pass', 'host:8080:user:',
    'http://host:8080:user:pass', 'host:port:user:pass', 'host:8080:user', '2001:db8::1:8080:user:pass'
  ])('rejects malformed input: %s', line => {
    expect(parseProxyUrl(line)).toBeNull()
  })
})
