import { NextApiRequest, NextApiResponse } from 'next'

import { apiWrapper } from '@/lib/api/apiWrapper'

/**
 * Reads the HA agent's view of the replication group.
 *
 * The agents listen on a port inside the server's network that a browser
 * cannot reach, so Studio fetches it here, server-side.
 *
 * Read-only on purpose. Promoting a standby is destructive and irreversible,
 * and self-hosted Studio has no authentication of its own — `withAuth` is a
 * no-op when IS_PLATFORM is false, so the only gate is Kong's dashboard
 * password. Exposing promotion here would silently turn "knows the HA token"
 * into "knows the dashboard password" as the authority to fail over a
 * database. Promotion stays on the agent's own page, which asks for the token.
 */
export default function haCluster(req: NextApiRequest, res: NextApiResponse) {
  return apiWrapper(req, res, handler)
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { method } = req

  switch (method) {
    case 'GET':
      return handleGet(req, res)
    default:
      res.setHeader('Allow', ['GET'])
      return res
        .status(405)
        .json({ data: null, error: { message: `Method ${method} Not Allowed` } })
  }
}

const handleGet = async (_req: NextApiRequest, res: NextApiResponse) => {
  const agentUrl = process.env.HA_AGENT_URL

  // Not configured is a normal state, not a failure: high availability is
  // opt-in, and most self-hosted installs never turn it on.
  if (!agentUrl) {
    return res.status(501).json({
      data: null,
      error: {
        message:
          'HA_AGENT_URL is not set. High availability is optional — see docker/ha/README.md to enable it.',
      },
    })
  }

  try {
    const response = await fetch(`${agentUrl.replace(/\/+$/, '')}/cluster`, {
      headers: { accept: 'application/json' },
      // The agent answers in milliseconds when healthy. A hung request here
      // would otherwise hold a Studio worker for the platform default.
      signal: AbortSignal.timeout(10_000),
    })

    if (!response.ok) {
      return res.status(502).json({
        data: null,
        error: { message: `HA agent responded with ${response.status}` },
      })
    }

    return res.status(200).json(await response.json())
  } catch (error) {
    // An unreachable agent is itself news worth showing, so report it as data
    // rather than letting it surface as an opaque 500.
    const message = error instanceof Error ? error.message : 'unknown error'
    return res.status(502).json({
      data: null,
      error: { message: `Could not reach the HA agent at ${agentUrl}: ${message}` },
    })
  }
}
