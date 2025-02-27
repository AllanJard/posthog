import { LemonTag } from '@posthog/lemon-ui'
import { useValues } from 'kea'
import React, { useMemo } from 'react'
import { JobSpec } from '~/types'
import {
    HISTORICAL_EXPORT_JOB_NAME,
    HISTORICAL_EXPORT_JOB_NAME_V2,
    PluginJobConfiguration,
} from './PluginJobConfiguration'
import { featureFlagLogic } from 'lib/logic/featureFlagLogic'
import { FEATURE_FLAGS } from '../../../../lib/constants'

interface PluginJobOptionsProps {
    pluginId: number
    pluginConfigId: number
    capabilities: Record<'jobs' | 'methods' | 'scheduled_tasks', string[]>
    publicJobs: Record<string, JobSpec>
}

export function PluginJobOptions({
    pluginId,
    pluginConfigId,
    capabilities,
    publicJobs,
}: PluginJobOptionsProps): JSX.Element {
    const { featureFlags } = useValues(featureFlagLogic)

    const jobs = useMemo(() => {
        return capabilities.jobs
            .filter((jobName) => jobName in publicJobs)
            .filter((jobName) => {
                // Hide either old or new export depending on the feature flag value
                if (jobName === HISTORICAL_EXPORT_JOB_NAME && featureFlags[FEATURE_FLAGS.HISTORICAL_EXPORTS_V2]) {
                    return false
                } else if (
                    jobName === HISTORICAL_EXPORT_JOB_NAME_V2 &&
                    !featureFlags[FEATURE_FLAGS.HISTORICAL_EXPORTS_V2]
                ) {
                    return false
                }

                return true
            })
    }, [capabilities, publicJobs, featureFlags])

    return (
        <>
            <h3 className="l3" style={{ marginTop: 32 }}>
                Jobs
                <LemonTag type="warning" style={{ verticalAlign: '0.125em', marginLeft: 6 }}>
                    BETA
                </LemonTag>
            </h3>

            {jobs.map((jobName) => (
                <div key={jobName}>
                    {jobName.includes('Export historical events') ? <i>Export historical events</i> : <i>{jobName}</i>}
                    <PluginJobConfiguration
                        jobName={jobName}
                        jobSpec={publicJobs[jobName]}
                        pluginConfigId={pluginConfigId}
                        pluginId={pluginId}
                    />
                </div>
            ))}
        </>
    )
}
