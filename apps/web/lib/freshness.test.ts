// lib/freshness.test.ts: run with `npm test`.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { isEnded, isLive, notLiveSummary, oldDataMarker, shortAge } from './freshness.ts'

const NOW = Date.UTC(2026, 10, 3, 16, 0)
const MIN = 60_000, HOUR = 60 * MIN
const open = { status: 'open', gammaClosed: false, acceptingOrders: true, endMs: NOW + 24 * HOUR, lastTradeMs: NOW - MIN }

test('ended: closed or resolved, ours or Gamma\'s', () => {
  assert.equal(isEnded({ ...open, status: 'closed' }, NOW), true)
  assert.equal(isEnded({ ...open, status: 'resolved' }, NOW), true)
  assert.equal(isEnded({ ...open, gammaClosed: true }, NOW), true)
})

test('ended: not accepting orders', () => {
  assert.equal(isEnded({ ...open, acceptingOrders: false }, NOW), true)
})

test('open market before its end date is not ended', () => {
  assert.equal(isEnded(open, NOW), false)
})

test('past end date but still trading is not ended (awaiting resolution)', () => {
  assert.equal(isEnded({ ...open, endMs: NOW - 3 * 24 * HOUR, lastTradeMs: NOW - 8 * MIN }, NOW), false)
  assert.equal(isEnded({ ...open, endMs: NOW - HOUR, lastTradeMs: NOW - 3 * HOUR }, NOW), false) // exactly 3 h: kept
})

test('past end date and quiet for over 3 hours is ended', () => {
  assert.equal(isEnded({ ...open, endMs: NOW - 12 * HOUR, lastTradeMs: NOW - 3 * HOUR - MIN }, NOW), true)
  assert.equal(isEnded({ ...open, endMs: NOW - HOUR, lastTradeMs: null }, NOW), true)
})

test('no end date: only the status rules apply', () => {
  assert.equal(isEnded({ ...open, endMs: null, lastTradeMs: null }, NOW), false)
})

test('live needs at least 10 bars in the window, like the worker', () => {
  assert.equal(isLive(9), false)
  assert.equal(isLive(10), true)
})

test('old-data marker only after 30 minutes', () => {
  assert.equal(oldDataMarker(NOW - 30 * MIN, NOW), null)
  assert.equal(oldDataMarker(NOW - 45 * MIN, NOW), 'data 45m old')
  assert.equal(oldDataMarker(NOW - 2 * HOUR - 10 * MIN, NOW), 'data 2h old')
  assert.equal(oldDataMarker(null, NOW), null)
})

test('short ages', () => {
  assert.equal(shortAge(59 * MIN), '59m')
  assert.equal(shortAge(25 * HOUR), '1d')
})

test('not-live summary, singular and plural, no em dashes', () => {
  assert.equal(notLiveSummary(1), '1 market not live (too little trading in the last 4 hours to score)')
  assert.equal(notLiveSummary(41), '41 markets not live (too little trading in the last 4 hours to score)')
  assert.ok(!notLiveSummary(41).includes('—'))
})
