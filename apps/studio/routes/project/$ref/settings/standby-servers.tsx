import { createFileRoute } from '@tanstack/react-router'

import ProjectStandbyServers from '@/pages/project/[ref]/settings/standby-servers'

export const Route = createFileRoute('/project/$ref/settings/standby-servers')({
  component: SettingsStandbyServersRoute,
  staticData: { settingsLayoutTitle: 'Standby servers' },
})

function SettingsStandbyServersRoute() {
  return <ProjectStandbyServers dehydratedState={undefined} />
}
