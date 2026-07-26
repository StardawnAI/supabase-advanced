import type { HaNode } from '@/data/ha/ha-cluster-query'

/**
 * Helpers for the self-hosted standby-server view.
 *
 * Named "standby servers" rather than "high availability" on purpose: Studio
 * already has a High Availability feature of its own for platform projects
 * (`hooks/misc/useHighAvailability.ts`), and reusing that name would both
 * confuse operators and put this fork on a collision course with upstream.
 */

/** Renders a WAL byte distance the way an operator reads it. */
export function formatLagBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return 'unknown'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

/** One line summarising what a node is doing, in the operator's terms. */
export function describeReplication(node: HaNode): string {
  if (node.role === 'primary') {
    const count = node.replicas?.length ?? 0
    if (count === 0) return 'No standby connected'
    return count === 1 ? '1 standby streaming' : `${count} standbys streaming`
  }
  if (node.role === 'standby') {
    if (!node.streaming) return 'Not streaming'
    const seconds = node.lag_seconds ?? 0
    return `Streaming, ${seconds.toFixed(1)}s behind (${formatLagBytes(node.lag_bytes)})`
  }
  if (node.role === 'witness') return 'Observer only, holds no data'
  if (node.role === 'unreachable') return 'Agent did not answer'
  if (node.role === 'down') return 'Database not reachable from its agent'
  return '—'
}

export type StandbyGroupHealth =
  | { status: 'ok'; primary: string }
  | { status: 'split-brain'; primaries: string[] }
  | { status: 'no-primary' }

/**
 * Reduces the group to the one thing an operator needs to know first.
 *
 * Two primaries is the state that silently destroys data — both are taking
 * writes and the two histories can no longer be reconciled — so it outranks
 * everything else, including nodes being down.
 */
export function summariseHealth(nodes: HaNode[]): StandbyGroupHealth {
  const primaries = nodes.filter((node) => node.role === 'primary')
  if (primaries.length > 1) {
    return { status: 'split-brain', primaries: primaries.map((node) => node.node) }
  }
  if (primaries.length === 0) return { status: 'no-primary' }
  return { status: 'ok', primary: primaries[0].node }
}
