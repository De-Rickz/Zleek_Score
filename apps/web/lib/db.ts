// lib/db.ts
import 'server-only' // build fails if a client component ever imports this
import postgres from 'postgres'

const url = process.env.DATABASE_URL
if (!url) throw new Error('DATABASE_URL is not set (apps/web/.env.local)')

// Next dev hot-reloads modules. Without this singleton, every reload opens a
// new pool and you hit "too many clients" within minutes.
const g = globalThis as unknown as { __sql?: ReturnType<typeof postgres> }

export const sql =
  g.__sql ?? postgres(url, { max: 5, idle_timeout: 20, connect_timeout: 10 })

if (process.env.NODE_ENV !== 'production') g.__sql = sql