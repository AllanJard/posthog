import './Insight.scss'
import React, { useEffect } from 'react'
import { BindLogic, useActions, useMountedLogic, useValues } from 'kea'
import { insightSceneLogic } from 'scenes/insights/insightSceneLogic'
import { insightLogic } from './insightLogic'
import { insightCommandLogic } from './insightCommandLogic'
import { AvailableFeature, ExporterFormat, InsightModel, InsightShortId, InsightType, ItemMode } from '~/types'
import { NPSPrompt } from 'lib/experimental/NPSPrompt'
import { SaveCohortModal } from 'scenes/trends/persons-modal/SaveCohortModal'
import { personsModalLogic } from 'scenes/trends/persons-modal/personsModalLogic'
import { InsightsNav } from './InsightsNav'
import { AddToDashboard } from 'lib/components/AddToDashboard/AddToDashboard'
import { InsightContainer } from 'scenes/insights/InsightContainer'
import { EditableField } from 'lib/components/EditableField/EditableField'
import { ObjectTags } from 'lib/components/ObjectTags/ObjectTags'
import { InsightSaveButton } from './InsightSaveButton'
import { userLogic } from 'scenes/userLogic'
import { FeedbackCallCTA } from 'lib/experimental/FeedbackCallCTA'
import { PageHeader } from 'lib/components/PageHeader'
import { IconLock } from 'lib/components/icons'
import { summarizeInsightFilters } from './utils'
import { groupsModel } from '~/models/groupsModel'
import { cohortsModel } from '~/models/cohortsModel'
import { mathsLogic } from 'scenes/trends/mathsLogic'
import { InsightSkeleton } from 'scenes/insights/InsightSkeleton'
import { LemonButton } from 'lib/components/LemonButton'
import { featureFlagLogic } from 'lib/logic/featureFlagLogic'
import { FEATURE_FLAGS } from 'lib/constants'
import { EditorFilters } from './EditorFilters/EditorFilters'
import { More } from 'lib/components/LemonButton/More'
import { LemonDivider } from 'lib/components/LemonDivider'
import { deleteWithUndo } from 'lib/utils'
import { teamLogic } from 'scenes/teamLogic'
import { savedInsightsLogic } from 'scenes/saved-insights/savedInsightsLogic'
import { router } from 'kea-router'
import { urls } from 'scenes/urls'
import { SubscribeButton, SubscriptionsModal } from 'lib/components/Subscriptions/SubscriptionsModal'
import { UserActivityIndicator } from 'lib/components/UserActivityIndicator/UserActivityIndicator'
import clsx from 'clsx'
import { SharingModal } from 'lib/components/Sharing/SharingModal'
import { ExportButton } from 'lib/components/ExportButton/ExportButton'
import { useDebouncedCallback } from 'use-debounce'
import { AlertMessage } from 'lib/components/AlertMessage'
import { Link } from '@posthog/lemon-ui'

export function Insight({ insightId }: { insightId: InsightShortId | 'new' }): JSX.Element {
    const { insightMode, subscriptionId } = useValues(insightSceneLogic)
    const { setInsightMode } = useActions(insightSceneLogic)
    const { featureFlags } = useValues(featureFlagLogic)
    const { currentTeamId } = useValues(teamLogic)
    const { push } = useActions(router)

    const logic = insightLogic({ dashboardItemId: insightId || 'new' })
    const {
        insightProps,
        insightLoading,
        filtersKnown,
        filters,
        canEditInsight,
        insight,
        insightChanged,
        tagLoading,
        insightSaving,
        exporterResourceParams,
    } = useValues(logic)
    useMountedLogic(insightCommandLogic(insightProps))
    const { saveInsight, setInsightMetadata, saveAs, reportInsightViewedForRecentInsights } = useActions(logic)
    const { duplicateInsight, loadInsights } = useActions(savedInsightsLogic)

    const { hasAvailableFeature } = useValues(userLogic)
    const { cohortModalVisible } = useValues(personsModalLogic)
    const { saveCohortWithUrl, setCohortModalVisible } = useActions(personsModalLogic)
    const { aggregationLabel } = useValues(groupsModel)
    const { cohortsById } = useValues(cohortsModel)
    const { mathDefinitions } = useValues(mathsLogic)

    useEffect(() => {
        reportInsightViewedForRecentInsights()
    }, [insightId])

    const usingEditorPanels = featureFlags[FEATURE_FLAGS.INSIGHT_EDITOR_PANELS]
    const actorOnEventsQueryingEnabled = featureFlags[FEATURE_FLAGS.ACTOR_ON_EVENTS_QUERYING]

    const debouncedOnChange = useDebouncedCallback((insightMetadata) => {
        if (insightMode === ItemMode.Edit) {
            setInsightMetadata(insightMetadata)
        }
    }, 250)

    // Show the skeleton if loading an insight for which we only know the id
    // This helps with the UX flickering and showing placeholder "name" text.
    if (insightId !== 'new' && insightLoading && !filtersKnown) {
        return <InsightSkeleton />
    }

    const insightScene = (
        <div className={'insights-page'}>
            {insightId !== 'new' && (
                <>
                    <SubscriptionsModal
                        isOpen={insightMode === ItemMode.Subscriptions}
                        closeModal={() => push(urls.insightView(insight.short_id as InsightShortId))}
                        insightShortId={insightId}
                        subscriptionId={subscriptionId}
                    />

                    <SharingModal
                        isOpen={insightMode === ItemMode.Sharing}
                        closeModal={() => push(urls.insightView(insight.short_id as InsightShortId))}
                        insightShortId={insightId}
                        insight={insight}
                    />
                </>
            )}
            <PageHeader
                title={
                    <EditableField
                        name="name"
                        value={insight.name || ''}
                        placeholder={summarizeInsightFilters(filters, aggregationLabel, cohortsById, mathDefinitions)}
                        onSave={(value) => setInsightMetadata({ name: value })}
                        maxLength={400} // Sync with Insight model
                        // lock into edit without buttons when insight is editing
                        mode={!canEditInsight ? 'view' : insightMode === ItemMode.Edit ? 'edit' : undefined}
                        onChange={(value) => {
                            debouncedOnChange({ name: value })
                        }}
                        data-attr="insight-name"
                        notice={
                            !canEditInsight
                                ? {
                                      icon: <IconLock />,
                                      tooltip:
                                          "You don't have edit permissions on any of the dashboards this insight belongs to. Ask a dashboard collaborator with edit access to add you.",
                                  }
                                : undefined
                        }
                        // Don't autofocus when we enter edit mode - this field is not of prime concern then
                        autoFocus={insightMode !== ItemMode.Edit}
                    />
                }
                buttons={
                    <div className="flex justify-between items-center gap-2">
                        {insightMode !== ItemMode.Edit && (
                            <>
                                <More
                                    overlay={
                                        <>
                                            <LemonButton
                                                status="stealth"
                                                onClick={() => duplicateInsight(insight as InsightModel, true)}
                                                fullWidth
                                            >
                                                Duplicate
                                            </LemonButton>
                                            <LemonButton
                                                status="stealth"
                                                onClick={() =>
                                                    setInsightMetadata({
                                                        favorited: !insight.favorited,
                                                    })
                                                }
                                                fullWidth
                                            >
                                                {insight.favorited ? 'Remove from favorites' : 'Add to favorites'}
                                            </LemonButton>
                                            <LemonDivider />

                                            <LemonButton
                                                status="stealth"
                                                onClick={() =>
                                                    insight.short_id
                                                        ? push(urls.insightSharing(insight.short_id))
                                                        : null
                                                }
                                                fullWidth
                                            >
                                                Share or embed
                                            </LemonButton>
                                            {insight.short_id && (
                                                <>
                                                    <SubscribeButton insightShortId={insight.short_id} />
                                                    {exporterResourceParams ? (
                                                        <ExportButton
                                                            fullWidth
                                                            items={[
                                                                {
                                                                    export_format: ExporterFormat.PNG,
                                                                    insight: insight.id,
                                                                },
                                                                {
                                                                    export_format: ExporterFormat.CSV,
                                                                    export_context: exporterResourceParams,
                                                                },
                                                            ]}
                                                        />
                                                    ) : null}
                                                    <LemonDivider />
                                                </>
                                            )}

                                            <LemonButton
                                                status="danger"
                                                onClick={() =>
                                                    deleteWithUndo({
                                                        object: insight,
                                                        endpoint: `projects/${currentTeamId}/insights`,
                                                        callback: () => {
                                                            loadInsights()
                                                            push(urls.savedInsights())
                                                        },
                                                    })
                                                }
                                                fullWidth
                                            >
                                                Delete insight
                                            </LemonButton>
                                        </>
                                    }
                                />
                                <LemonDivider vertical />
                            </>
                        )}
                        {insightMode === ItemMode.Edit && insight.saved && (
                            <LemonButton type="secondary" onClick={() => setInsightMode(ItemMode.View, null)}>
                                Cancel
                            </LemonButton>
                        )}
                        {insightMode !== ItemMode.Edit && insight.short_id && (
                            <AddToDashboard insight={insight} canEditInsight={canEditInsight} />
                        )}
                        {insightMode !== ItemMode.Edit ? (
                            canEditInsight && (
                                <LemonButton
                                    type="primary"
                                    onClick={() => setInsightMode(ItemMode.Edit, null)}
                                    data-attr="insight-edit-button"
                                >
                                    Edit
                                </LemonButton>
                            )
                        ) : (
                            <InsightSaveButton
                                saveAs={saveAs}
                                saveInsight={saveInsight}
                                isSaved={insight.saved}
                                addingToDashboard={!!insight.dashboards?.length && !insight.id}
                                insightSaving={insightSaving}
                                insightChanged={insightChanged}
                            />
                        )}
                    </div>
                }
                caption={
                    <>
                        {!!(canEditInsight || insight.description) && (
                            <EditableField
                                className="my-3"
                                multiline
                                name="description"
                                value={insight.description || ''}
                                placeholder="Description (optional)"
                                onSave={(value) => setInsightMetadata({ description: value })}
                                maxLength={400} // Sync with Insight model
                                // lock into edit without buttons when insight is editing
                                mode={!canEditInsight ? 'view' : insightMode === ItemMode.Edit ? 'edit' : undefined}
                                onChange={(value) => {
                                    debouncedOnChange({ description: value })
                                }}
                                data-attr="insight-description"
                                compactButtons
                                paywall={!hasAvailableFeature(AvailableFeature.DASHBOARD_COLLABORATION)}
                                // Don't autofocus when we enter edit mode - this field is not of prime concern then
                                autoFocus={insightMode !== ItemMode.Edit}
                            />
                        )}
                        {canEditInsight ? (
                            <ObjectTags
                                tags={insight.tags ?? []}
                                onChange={(_, tags) => setInsightMetadata({ tags: tags ?? [] })}
                                saving={tagLoading}
                                tagsAvailable={[]}
                                className="insight-metadata-tags"
                                data-attr="insight-tags"
                            />
                        ) : insight.tags?.length ? (
                            <ObjectTags
                                tags={insight.tags}
                                saving={tagLoading}
                                className="insight-metadata-tags"
                                data-attr="insight-tags"
                                staticOnly
                            />
                        ) : null}
                        <UserActivityIndicator
                            at={insight.last_modified_at}
                            by={insight.last_modified_by}
                            className="mt-2"
                        />
                    </>
                }
            />

            {actorOnEventsQueryingEnabled ? (
                <div className="mb-4">
                    <AlertMessage type="info">
                        To speed up queries, we've adjusted how they're calculated. You might notice some differences in
                        the insight results. Read more about what changes to expect{' '}
                        <Link to={`https://posthog.com/docs/how-posthog-works/queries`}>here</Link>. Please{' '}
                        <Link to={'https://posthog.com/support/'}>contact us</Link> if you have any further questions
                        regarding the changes
                    </AlertMessage>
                </div>
            ) : null}

            {!usingEditorPanels && insightMode === ItemMode.Edit && <InsightsNav />}

            <div
                className={clsx('insight-wrapper', {
                    'insight-wrapper--editorpanels': usingEditorPanels,
                    'insight-wrapper--singlecolumn': !usingEditorPanels && filters.insight === InsightType.FUNNELS,
                })}
            >
                <EditorFilters insightProps={insightProps} showing={insightMode === ItemMode.Edit} />
                <div className="insights-container" data-attr="insight-view">
                    {<InsightContainer />}
                </div>
            </div>

            {insightMode !== ItemMode.View ? (
                <>
                    <NPSPrompt />
                    <FeedbackCallCTA />
                </>
            ) : null}

            <SaveCohortModal
                isOpen={cohortModalVisible}
                onSave={(title: string) => {
                    saveCohortWithUrl(title)
                    setCohortModalVisible(false)
                }}
                onCancel={() => setCohortModalVisible(false)}
            />
        </div>
    )

    return (
        <BindLogic logic={insightLogic} props={insightProps}>
            {insightScene}
        </BindLogic>
    )
}
