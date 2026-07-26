import { describe, expect, it } from 'vitest'

import { describeReplication, formatLagBytes, summariseHealth } from '../StandbyServers.utils'
import type { HaNode } from '@/data/ha/ha-cluster-query'

const node = (overrides: Partial<HaNode> & Pick<HaNode, 'node' | 'role'>): HaNode => ({
  ...overrides,
})

describe('formatLagBytes', () => {
  it('scales to the unit an operator can read at a glance', () => {
    expect(formatLagBytes(512)).toBe('512 B')
    expect(formatLagBytes(2048)).toBe('2.0 KB')
    expect(formatLagBytes(5 * 1024 * 1024)).toBe('5.0 MB')
    expect(formatLagBytes(3 * 1024 * 1024 * 1024)).toBe('3.00 GB')
  })

  it('says so when the agent reported no figure, rather than showing 0', () => {
    // A missing value and a genuinely caught-up standby mean very different
    // things, so they must not render the same.
    expect(formatLagBytes(undefined)).toBe('unknown')
    expect(formatLagBytes(null)).toBe('unknown')
    expect(formatLagBytes(0)).toBe('0 B')
  })
})

describe('describeReplication', () => {
  it('tells a primary with no standby apart from one that has them', () => {
    // The dangerous case: replication was set up but is no longer running.
    expect(describeReplication(node({ node: 'a', role: 'primary', replicas: [] }))).toBe(
      'No standby connected'
    )
    expect(
      describeReplication(node({ node: 'a', role: 'primary', replicas: [{ state: 'streaming' }] }))
    ).toBe('1 standby streaming')
    expect(
      describeReplication(
        node({
          node: 'a',
          role: 'primary',
          replicas: [{ state: 'streaming' }, { state: 'catchup' }],
        })
      )
    ).toBe('2 standbys streaming')
  })

  it('reports a standby that has stopped streaming', () => {
    expect(describeReplication(node({ node: 'b', role: 'standby', streaming: false }))).toBe(
      'Not streaming'
    )
  })

  it('gives both the time and the byte distance for a live standby', () => {
    expect(
      describeReplication(
        node({ node: 'b', role: 'standby', streaming: true, lag_seconds: 0.27, lag_bytes: 4096 })
      )
    ).toBe('Streaming, 0.3s behind (4.0 KB)')
  })

  it('describes the non-database roles', () => {
    expect(describeReplication(node({ node: 'w', role: 'witness' }))).toBe(
      'Observer only, holds no data'
    )
    expect(describeReplication(node({ node: 'c', role: 'unreachable' }))).toBe(
      'Agent did not answer'
    )
    expect(describeReplication(node({ node: 'd', role: 'down' }))).toBe(
      'Database not reachable from its agent'
    )
  })
})

describe('summariseHealth', () => {
  it('reports the primary when exactly one node is serving writes', () => {
    const health = summariseHealth([
      node({ node: 'a', role: 'primary' }),
      node({ node: 'b', role: 'standby' }),
      node({ node: 'w', role: 'witness' }),
    ])
    expect(health).toEqual({ status: 'ok', primary: 'a' })
  })

  it('flags two primaries, naming both', () => {
    // This is the state that loses data silently: both are accepting writes
    // and the two histories can no longer be merged.
    const health = summariseHealth([
      node({ node: 'a', role: 'primary' }),
      node({ node: 'b', role: 'primary' }),
    ])
    expect(health).toEqual({ status: 'split-brain', primaries: ['a', 'b'] })
  })

  it('flags a group with no primary at all', () => {
    const health = summariseHealth([
      node({ node: 'a', role: 'standby' }),
      node({ node: 'b', role: 'unreachable' }),
    ])
    expect(health).toEqual({ status: 'no-primary' })
  })

  it('does not mistake an unreachable node for a second primary', () => {
    // An agent that cannot be reached says nothing about who is primary, so it
    // must not trigger the split-brain alarm.
    const health = summariseHealth([
      node({ node: 'a', role: 'primary' }),
      node({ node: 'b', role: 'unreachable' }),
    ])
    expect(health).toEqual({ status: 'ok', primary: 'a' })
  })
})
