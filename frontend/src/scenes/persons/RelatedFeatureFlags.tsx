import { LemonInput, LemonSelect, LemonTable, LemonTag, Link } from '@posthog/lemon-ui'
import { useActions, useValues } from 'kea'
import { LemonTableColumns } from 'lib/components/LemonTable'
import { normalizeColumnTitle } from 'lib/components/Table/utils'
import { capitalizeFirstLetter } from 'lib/utils'
import stringWithWBR from 'lib/utils/stringWithWBR'
import React from 'react'
import { urls } from 'scenes/urls'
import { FeatureFlagReleaseType } from '~/types'
import { relatedFeatureFlagsLogic, RelatedFeatureFlag } from './relatedFeatureFlagsLogic'

interface Props {
    distinctId: string
}

export enum FeatureFlagMatchReason {
    ConditionMatch = 'condition_match',
    NoConditionMatch = 'no_condition_match',
    OutOfRolloutBound = 'out_of_rollout_bound',
    NoGroupType = 'no_group_type',
    Disabled = 'disabled',
}

const featureFlagMatchMapping = {
    [FeatureFlagMatchReason.ConditionMatch]: 'Matches',
    [FeatureFlagMatchReason.NoConditionMatch]: "Doesn't match any conditions",
    [FeatureFlagMatchReason.OutOfRolloutBound]: 'Out of rollout bound',
    [FeatureFlagMatchReason.NoGroupType]: 'Missing group type',
    [FeatureFlagMatchReason.Disabled]: 'Disabled',
}

export function RelatedFeatureFlags({ distinctId }: Props): JSX.Element {
    const { filteredMappedFlags, relatedFeatureFlagsLoading, searchTerm, filters } = useValues(
        relatedFeatureFlagsLogic({ distinctId })
    )
    const { setSearchTerm, setFilters } = useActions(relatedFeatureFlagsLogic({ distinctId }))

    const columns: LemonTableColumns<RelatedFeatureFlag> = [
        {
            title: normalizeColumnTitle('Key'),
            dataIndex: 'key',
            className: 'ph-no-capture',
            sticky: true,
            width: '40%',
            sorter: (a: RelatedFeatureFlag, b: RelatedFeatureFlag) => (a.key || '').localeCompare(b.key || ''),
            render: function Render(_, featureFlag: RelatedFeatureFlag) {
                const isExperiment = (featureFlag.experiment_set || []).length > 0
                return (
                    <>
                        <Link to={featureFlag.id ? urls.featureFlag(featureFlag.id) : undefined} className="row-name">
                            {stringWithWBR(featureFlag.key, 17)}
                            <LemonTag type={isExperiment ? 'purple' : 'default'} className="ml-2">
                                {isExperiment ? 'Experiment' : 'Feature flag'}
                            </LemonTag>
                        </Link>
                        {featureFlag.name && <span className="row-description">{featureFlag.name}</span>}
                    </>
                )
            },
        },
        {
            title: 'Type',
            width: 100,
            render: function Render(_, featureFlag: RelatedFeatureFlag) {
                return featureFlag.filters.multivariate
                    ? FeatureFlagReleaseType.Variants
                    : FeatureFlagReleaseType.ReleaseToggle
            },
        },
        {
            title: 'Value',
            dataIndex: 'value',
            width: 100,
            render: function Render(_, featureFlag: RelatedFeatureFlag) {
                return <div>{capitalizeFirstLetter(featureFlag.value.toString())}</div>
            },
        },
        {
            title: 'Match evaluation',
            dataIndex: 'evaluation',
            width: 150,
            render: function Render(_, featureFlag: RelatedFeatureFlag) {
                const matchesSet = featureFlag.evaluation.reason === FeatureFlagMatchReason.ConditionMatch
                return (
                    <div>
                        {featureFlag.active ? (
                            <>
                                {matchesSet
                                    ? featureFlagMatchMapping[FeatureFlagMatchReason.ConditionMatch]
                                    : featureFlagMatchMapping[FeatureFlagMatchReason.NoConditionMatch]}
                            </>
                        ) : (
                            '--'
                        )}

                        {matchesSet && (
                            <span className="simple-tag ml-2" style={{ background: 'var(--primary-highlight)' }}>
                                Set {(featureFlag.evaluation.condition_index ?? 0) + 1}
                            </span>
                        )}
                    </div>
                )
            },
        },
        {
            title: 'Status',
            dataIndex: 'active',
            sorter: (a: RelatedFeatureFlag, b: RelatedFeatureFlag) => Number(a.active) - Number(b.active),
            width: 100,
            render: function RenderActive(_, featureFlag: RelatedFeatureFlag) {
                return <span className="font-normal">{featureFlag.active ? 'Enabled' : 'Disabled'}</span>
            },
        },
    ]
    return (
        <>
            <div className="flex justify-between mb-4">
                <LemonInput
                    type="search"
                    placeholder="Search for feature flags"
                    onChange={setSearchTerm}
                    value={searchTerm}
                />
                <div className="flex items-center gap-2">
                    <span>
                        <b>Type</b>
                    </span>
                    <LemonSelect
                        options={[
                            { label: 'All types', value: 'all' },
                            {
                                label: FeatureFlagReleaseType.ReleaseToggle,
                                value: FeatureFlagReleaseType.ReleaseToggle,
                            },
                            { label: FeatureFlagReleaseType.Variants, value: FeatureFlagReleaseType.Variants },
                        ]}
                        onChange={(type) => {
                            if (type) {
                                if (type === 'all') {
                                    if (filters) {
                                        const { type, ...restFilters } = filters
                                        setFilters(restFilters, true)
                                    }
                                } else {
                                    setFilters({ type })
                                }
                            }
                        }}
                        value="all"
                        dropdownMaxContentWidth
                    />
                    <span className="ml-2">
                        <b>Match evaluation</b>
                    </span>
                    <LemonSelect
                        options={[
                            { label: 'All', value: 'all' },
                            { label: 'Matched', value: FeatureFlagMatchReason.ConditionMatch },
                            { label: 'Not matched', value: 'not matched' },
                        ]}
                        onChange={(reason) => {
                            if (reason) {
                                if (reason === 'all') {
                                    if (filters) {
                                        const { reason, ...restFilters } = filters
                                        setFilters(restFilters, true)
                                    }
                                } else {
                                    setFilters({ reason })
                                }
                            }
                        }}
                        value="all"
                        dropdownMaxContentWidth
                    />
                    <span className="ml-2">
                        <b>Status</b>
                    </span>
                    <LemonSelect
                        onChange={(status) => {
                            if (status) {
                                if (status === 'all') {
                                    if (filters) {
                                        const { active, ...restFilters } = filters
                                        setFilters(restFilters, true)
                                    }
                                } else {
                                    setFilters({ active: status })
                                }
                            }
                        }}
                        options={[
                            { label: 'All', value: 'all' },
                            { label: 'Enabled', value: 'true' },
                            { label: 'Disabled', value: 'false' },
                        ]}
                        value="all"
                        dropdownMaxContentWidth
                    />
                </div>
            </div>
            <LemonTable columns={columns} loading={relatedFeatureFlagsLoading} dataSource={filteredMappedFlags} />
        </>
    )
}
