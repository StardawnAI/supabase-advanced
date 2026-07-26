export const haKeys = {
  // Not scoped by project ref: high availability is a property of the server
  // this Studio runs on, not of a project inside it.
  cluster: () => ['ha', 'cluster'] as const,
}
