// lib/priceChange.test.ts: run with `npm test` (Node's built-in runner; Node
// 24 runs TypeScript directly).
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { describeChange } from './priceChange.ts'

const hhmm = (ms: number) => new Date(ms).toISOString().slice(11, 16)
const REF = Date.UTC(2026, 10, 3, 13, 22)

test('rise: up arrow and plus sign', () => {
  const c = describeChange({ cur_close: 0.47, ref_close: 0.43, ref_ms: REF }, hhmm)
  assert.equal(c.kind, 'change')
  assert.equal(c.kind === 'change' && c.text, '▲ +0.040')
  assert.equal(c.kind === 'change' && c.arrow, '▲')
  assert.equal(c.kind === 'change' && c.value, '+0.040')
  assert.equal(c.kind === 'change' && c.label, 'Change since 13:22, the last price at or before 60 minutes ago')
})

test('fall: down arrow and minus sign', () => {
  const c = describeChange({ cur_close: 0.41, ref_close: 0.43, ref_ms: REF }, hhmm)
  assert.equal(c.kind === 'change' && c.text, '▼ −0.020')
})

test('unchanged: no arrow', () => {
  const c = describeChange({ cur_close: 0.43, ref_close: 0.43, ref_ms: REF }, hhmm)
  assert.equal(c.kind === 'change' && c.text, '0.000')
  assert.equal(c.kind === 'change' && c.arrow, '')
})

test('float noise does not flip the sign', () => {
  const c = describeChange({ cur_close: 0.88000000000000004, ref_close: 0.88, ref_ms: REF }, hhmm)
  assert.equal(c.kind === 'change' && c.text, '0.000')
})

test('no trade in the last hour: dash with reason', () => {
  const c = describeChange({ cur_close: null, ref_close: 0.43, ref_ms: REF }, hhmm)
  assert.deepEqual(c, { kind: 'none', reason: 'No trades in the last hour' })
})

test('no reference in the 24 hours before: dash with reason', () => {
  const c = describeChange({ cur_close: 0.47, ref_close: null, ref_ms: null }, hhmm)
  assert.deepEqual(c, { kind: 'none', reason: 'No earlier price in the 24 hours before that' })
})

test('copy has no em dashes', () => {
  for (const row of [{ cur_close: 0.47, ref_close: 0.43, ref_ms: REF },
                     { cur_close: null, ref_close: null, ref_ms: null },
                     { cur_close: 0.47, ref_close: null, ref_ms: null }]) {
    assert.ok(!JSON.stringify(describeChange(row, hhmm)).includes('—'))
  }
})
