// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// $identity.id only resolves inside policy predicates; must use root's literal id here.
export const ROOT_IDENTITY = 'ffffffff-ffff-7fff-bfff-ffffffffffff'

export function uuid7(): string {
  const now = BigInt(Date.now())
  const bytes = new Uint8Array(16)
  for (let i = 0; i < 6; i++) bytes[5 - i] = Number((now >> BigInt(i * 8)) & 0xffn)
  const rand = crypto.getRandomValues(new Uint8Array(10))
  bytes[6] = 0x70 | (rand[0] & 0x0f)
  bytes[7] = rand[1]
  bytes[8] = 0x80 | (rand[2] & 0x3f)
  bytes.set(rand.subarray(3), 9)
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

function str(value: unknown): string {
  return value === undefined || value === null ? 'none' : `"${value}"`
}

function num(value: unknown): string {
  return value === undefined || value === null ? 'none' : String(value)
}

export function insertMonitorRql(id: string, input: Record<string, unknown>): string {
  return `INSERT uptime::monitors [{ \
		id: "${id}", owner: "${ROOT_IDENTITY}", name: ${str(input.name)}, kind: ${str(input.kind)}, \
		target: ${str(input.target)}, interval: ${input.interval_ms}ms, timeout: ${input.timeout_ms}ms, \
		http_method: ${str(input.http_method)}, expected_status: ${num(input.expected_status)}, \
		keyword: ${str(input.keyword)}, expected_ip: ${str(input.expected_ip)}, \
		failure_threshold: ${input.failure_threshold}, enabled: ${input.enabled}, \
		created_at: "2026-01-01T00:00:00Z", last_checked_at: none, consecutive_failures: 0, status: "unknown" \
	}]`
}
