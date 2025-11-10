// Deno Edge Function: upload-metrics
// Inserts metric rows into public.all_metrics using service role.
// Expects JSON array of records or { records: [...] }.
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

type Metric = {
  timestamp: string
  device_id: string
  temperature?: number | null
  main_pressure?: number | null
  billet_length?: number | null
  container_temp_front?: number | null
  container_temp_rear?: number | null
  production_counter?: number | null
  current_speed?: number | null
}

const ALLOWED_KEYS = new Set([
  'timestamp',
  'device_id',
  'temperature',
  'main_pressure',
  'billet_length',
  'container_temp_front',
  'container_temp_rear',
  'production_counter',
  'current_speed',
])

function sanitizeRecord(input: Record<string, unknown>): Metric | null {
  if (typeof input !== 'object' || input === null) return null
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(input)) {
    if (ALLOWED_KEYS.has(k)) {
      // Convert NaN to null and keep primitives only
      if (typeof v === 'number' && Number.isNaN(v)) out[k] = null
      else out[k] = v
    }
  }
  if (typeof out.timestamp !== 'string' || typeof out.device_id !== 'string') {
    return null
  }
  return out as Metric
}

function chunk<T>(arr: T[], size: number): T[][] {
  const res: T[][] = []
  for (let i = 0; i < arr.length; i += size) res.push(arr.slice(i, i + size))
  return res
}

Deno.serve(async (req) => {
  try {
    if (req.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 })
    }

    // SUPABASE_URL is injected by the platform; do not try to set it as a secret
    const url = Deno.env.get('SUPABASE_URL')
    // Use a custom secret name for the service role key (prefix SUPABASE_ is forbidden)
    const serviceKey = Deno.env.get('SERVICE_ROLE_KEY')
    if (!url || !serviceKey) {
      return new Response('Server misconfigured', { status: 500 })
    }

    const supabase = createClient(url, serviceKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    })

    const body = await req.json().catch(() => null)
    const recordsRaw: unknown = Array.isArray(body) ? body : body?.records
    if (!Array.isArray(recordsRaw)) {
      return new Response(JSON.stringify({ error: 'Expected array or {records: []}' }), { status: 400 })
    }

    const sanitized = recordsRaw
      .map((r) => sanitizeRecord(r as Record<string, unknown>))
      .filter((r): r is Metric => !!r)

    if (sanitized.length === 0) {
      return new Response(JSON.stringify({ inserted: 0, skipped: 0 }), { status: 200 })
    }

    let inserted = 0
    const batches = chunk(sanitized, 500)
    for (const batch of batches) {
      // Use upsert with onConflict to ignore duplicates
      const { error, count } = await supabase
        .from('all_metrics')
        .upsert(batch, { onConflict: 'timestamp,device_id', ignoreDuplicates: true, count: 'exact' })
      if (error) {
        return new Response(JSON.stringify({ error: error.message }), { status: 500 })
      }
      inserted += count ?? 0
    }

    return new Response(JSON.stringify({ inserted }), { status: 200 })
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500 })
  }
})
