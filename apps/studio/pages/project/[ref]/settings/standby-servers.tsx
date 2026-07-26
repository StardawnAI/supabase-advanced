import { StandbyServerList } from '@/components/interfaces/Settings/StandbyServers/StandbyServerList'
import { DefaultLayout } from '@/components/layouts/DefaultLayout'
import SettingsLayout from '@/components/layouts/ProjectSettingsLayout/SettingsLayout'
import {
  ScaffoldContainer,
  ScaffoldDescription,
  ScaffoldHeader,
  ScaffoldTitle,
} from '@/components/layouts/Scaffold'
import type { NextPageWithLayout } from '@/types'

const ProjectStandbyServers: NextPageWithLayout = () => {
  return (
    <>
      <ScaffoldContainer>
        <ScaffoldHeader>
          <ScaffoldTitle>Standby servers</ScaffoldTitle>
          <ScaffoldDescription>
            Live copies of this database on other servers, and which one is serving writes
          </ScaffoldDescription>
        </ScaffoldHeader>
      </ScaffoldContainer>
      <ScaffoldContainer>
        <StandbyServerList />
      </ScaffoldContainer>
    </>
  )
}

ProjectStandbyServers.getLayout = (page) => (
  <DefaultLayout>
    <SettingsLayout title="Standby servers">{page}</SettingsLayout>
  </DefaultLayout>
)

export default ProjectStandbyServers
