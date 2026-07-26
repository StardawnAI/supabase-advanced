import { useQuery } from '@tanstack/react-query'
import { Admonition } from 'ui-patterns/admonition'
import { GenericSkeletonLoader } from 'ui-patterns/ShimmeringLoader'

import { describeReplication, summariseHealth } from './StandbyServers.utils'
import {
  ScaffoldSection,
  ScaffoldSectionContent,
  ScaffoldSectionDescription,
  ScaffoldSectionDetail,
  ScaffoldSectionTitle,
} from '@/components/layouts/Scaffold'
import { haClusterQueryOptions, type HaNode } from '@/data/ha/ha-cluster-query'

const ROLE_LABELS: Record<string, string> = {
  primary: 'Primary',
  standby: 'Standby',
  witness: 'Witness',
  down: 'Down',
  unreachable: 'Unreachable',
}

const HEALTHY_ROLES = new Set(['primary', 'standby', 'witness'])

const NodeRow = ({ node }: { node: HaNode }) => {
  const isHealthy = HEALTHY_ROLES.has(node.role)
  const problem = node.error ?? node.last_error

  return (
    <div className="flex flex-col gap-2 border-b border-default px-6 py-4 last:border-b-0 md:flex-row md:items-center md:justify-between">
      <div className="min-w-0">
        <p className="text-sm text-foreground">
          {node.node}
          {node.is_self && <span className="text-foreground-lighter"> · this server</span>}
        </p>
        <p className="text-sm text-foreground-light">{describeReplication(node)}</p>
        {problem && <p className="mt-1 break-words text-xs text-destructive-600">{problem}</p>}
      </div>
      <p
        className={`shrink-0 text-sm ${isHealthy ? 'text-foreground-light' : 'text-destructive-600'}`}
      >
        {ROLE_LABELS[node.role] ?? node.role}
      </p>
    </div>
  )
}

export const StandbyServerList = () => {
  const { data, error, isPending, isError, isSuccess } = useQuery(haClusterQueryOptions())

  const health = isSuccess ? summariseHealth(data.nodes) : undefined

  return (
    <ScaffoldSection>
      <ScaffoldSectionDetail>
        <ScaffoldSectionTitle className="mb-2">Replication group</ScaffoldSectionTitle>
        <ScaffoldSectionDescription>
          Every server holding a copy of this database, and how far behind the copies are.
        </ScaffoldSectionDescription>
      </ScaffoldSectionDetail>

      <ScaffoldSectionContent>
        {isPending && <GenericSkeletonLoader />}

        {isError && (
          <Admonition type="default" title="No replication group is reporting">
            <p>{error.message}</p>
            <p className="mt-2">
              High availability is optional. To set up a live standby on a second server, see{' '}
              <code className="text-xs">docker/ha/README.md</code> in your Supabase directory.
            </p>
          </Admonition>
        )}

        {isSuccess && health?.status === 'split-brain' && (
          <Admonition type="destructive" title="Two servers both think they are the primary">
            <p>
              {health.primaries.join(' and ')} are each accepting writes. Their data is diverging
              and cannot be merged back afterwards.
            </p>
            <p className="mt-2">Stop one of them now, then rebuild it as a standby of the other.</p>
          </Admonition>
        )}

        {isSuccess && health?.status === 'no-primary' && (
          <Admonition type="destructive" title="No server is accepting writes">
            <p>
              Every node reports being a standby or is unreachable, so writes to this database are
              failing.
            </p>
          </Admonition>
        )}

        {isSuccess && (
          <div className="rounded-md border border-default bg-surface-100">
            {data.nodes.map((node) => (
              <NodeRow key={`${node.node}-${node.agent_url ?? 'self'}`} node={node} />
            ))}
          </div>
        )}

        {isSuccess && (
          <p className="mt-3 text-xs text-foreground-lighter">
            This view is read-only. Promoting a standby is done on the HA agent&apos;s own page,
            port 8008 on any node, which stays reachable when this server does not.
          </p>
        )}
      </ScaffoldSectionContent>
    </ScaffoldSection>
  )
}
