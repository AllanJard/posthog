import React from 'react'
import { useActions, useValues } from 'kea'
import { useInterval } from 'lib/hooks/useInterval'
import { CardContainer } from 'scenes/ingestion/CardContainer'
import { ingestionLogic } from 'scenes/ingestion/ingestionLogic'
import { teamLogic } from 'scenes/teamLogic'
import { Spinner } from 'lib/components/Spinner/Spinner'
import { LemonButton } from 'lib/components/LemonButton'
import './Panels.scss'
import { eventUsageLogic } from 'lib/utils/eventUsageLogic'
import { EventBufferNotice } from 'scenes/events/EventBufferNotice'
import { featureFlagLogic } from 'lib/logic/featureFlagLogic'
import { FEATURE_FLAGS } from 'lib/constants'

export function VerificationPanel(): JSX.Element {
    const { loadCurrentTeam } = useActions(teamLogic)
    const { currentTeam } = useValues(teamLogic)
    const { setAddBilling, completeOnboarding } = useActions(ingestionLogic)
    const { reportIngestionContinueWithoutVerifying } = useActions(eventUsageLogic)
    const { featureFlags } = useValues(featureFlagLogic)

    const shouldShowBilling = featureFlags[FEATURE_FLAGS.ONBOARDING_BILLING]

    useInterval(() => {
        if (!currentTeam?.ingested_event) {
            loadCurrentTeam()
        }
    }, 2000)

    return (
        <CardContainer>
            <div className="text-center">
                {!currentTeam?.ingested_event ? (
                    <>
                        <div className="ingestion-listening-for-events">
                            <Spinner className="text-4xl" />
                            <h1 className="ingestion-title pt-4">Listening for events...</h1>
                            <p className="prompt-text">
                                Once you have integrated the snippet and sent an event, we will verify it was properly
                                received and continue.
                            </p>
                            <EventBufferNotice className="mb-4" />
                            <LemonButton
                                fullWidth
                                center
                                type="secondary"
                                onClick={() => {
                                    if (shouldShowBilling) {
                                        setAddBilling(true)
                                    } else {
                                        completeOnboarding()
                                    }
                                    reportIngestionContinueWithoutVerifying()
                                }}
                            >
                                Continue without verifying
                            </LemonButton>
                        </div>
                    </>
                ) : (
                    <div>
                        <h1 className="ingestion-title">Successfully sent events!</h1>
                        <p className="prompt-text text-muted text-left">
                            You will now be able to explore PostHog and take advantage of all its features to understand
                            your users.
                        </p>
                        <div className="mb-4">
                            <LemonButton
                                data-attr="wizard-complete-button"
                                type="primary"
                                onClick={() => {
                                    if (shouldShowBilling) {
                                        setAddBilling(true)
                                    } else {
                                        completeOnboarding()
                                    }
                                }}
                                fullWidth
                                center
                            >
                                {shouldShowBilling ? 'Next' : 'Complete'}
                            </LemonButton>
                        </div>
                    </div>
                )}
            </div>
        </CardContainer>
    )
}
