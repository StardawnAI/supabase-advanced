import { createFileRoute } from '@tanstack/react-router'

import { toWebHandler } from '@/compat/next/api'
import nextHandler from '@/pages/api/ha/cluster'

const handler = toWebHandler(nextHandler)

export const Route = createFileRoute('/api/ha/cluster')({
  server: { handlers: { GET: handler } },
})
