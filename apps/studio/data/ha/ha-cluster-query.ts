import { queryOptions } from '@tanstack/react-query'
import { z } from 'zod'

import { haKeys } from './keys'
import { BASE_PATH, IS_PLATFORM } from '@/lib/constants'

/**
 * Status of the Postgres replication group behind this instance.
 *
 * Unlike the rest of `data/`, this does not go through `data/fetchers` — that
 * client is typed against the Supabase platform API, and the HA agents are not
 * part of it. It calls our own Next.js route instead, which proxies to the
 * agent (see `pages/api/ha/cluster.ts`).
 *
 * The agent is a small Python service, so its JSON is validated rather than
 * trusted: every field beyond the three the page cannot work without is
 * optional, so an older or newer agent degrades to missing detail instead of a
 * blank page.
 */

const haReplicaSchema = z.object({
  client_addr: z.string().nullish(),
  state: z.string().nullish(),
  sync_state: z.string().nullish(),
  lag_bytes: z.number().nullish(),
})

const haNodeSchema = z.object({
  node: z.string(),
  role: z.string(),
  healthy: z.boolean().nullish(),
  is_self: z.boolean().nullish(),
  agent_url: z.string().nullish(),
  upstream: z.string().nullish(),
  failover_mode: z.string().nullish(),
  streaming: z.boolean().nullish(),
  lag_seconds: z.number().nullish(),
  lag_bytes: z.number().nullish(),
  replicas: z.array(haReplicaSchema).nullish(),
  error: z.string().nullish(),
  last_error: z.string().nullish(),
})

const haClusterSchema = z.object({
  nodes: z.array(haNodeSchema),
  primary_count: z.number(),
  split_brain: z.boolean(),
})

const apiErrorSchema = z.object({ error: z.object({ message: z.string() }) })

export type HaReplica = z.infer<typeof haReplicaSchema>
export type HaNode = z.infer<typeof haNodeSchema>
export type HaCluster = z.infer<typeof haClusterSchema>

async function getHaCluster(signal?: AbortSignal): Promise<HaCluster> {
  const response = await fetch(`${BASE_PATH}/api/ha/cluster`, {
    headers: { accept: 'application/json' },
    signal,
  })

  const body: unknown = await response.json().catch(() => null)

  if (!response.ok) {
    const parsed = apiErrorSchema.safeParse(body)
    throw new Error(
      parsed.success ? parsed.data.error.message : `Request failed with status ${response.status}`
    )
  }

  return haClusterSchema.parse(body)
}

export const haClusterQueryOptions = () =>
  queryOptions({
    queryKey: haKeys.cluster(),
    queryFn: ({ signal }) => getHaCluster(signal),
    // High availability is a self-hosting feature; the platform manages
    // replication itself and exposes no agent.
    enabled: !IS_PLATFORM,
    // Replication lag is only meaningful while it is current.
    refetchInterval: 10_000,
    // The two failure modes here — not configured, agent unreachable — are
    // both worth showing immediately rather than hiding behind retries.
    retry: false,
  })
