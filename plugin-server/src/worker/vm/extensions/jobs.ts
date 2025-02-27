import { Hub, PluginConfig, PluginLogEntryType } from '../../../types'
import { JobName } from './../../../types'

type JobRunner = {
    runAt: (date: Date) => Promise<void>
    runIn: (duration: number, unit: string) => Promise<void>
    runNow: () => Promise<void>
}
type Job = (payload?: any) => JobRunner
type Jobs = Record<string, Job>

const milliseconds = 1
const seconds = 1000 * milliseconds
const minutes = 60 * seconds
const hours = 60 * minutes
const days = 24 * hours
const weeks = 7 * days
const months = 30 * weeks
const quarters = 13 * weeks
const years = 365 * days
const durations: Record<string, number> = {
    milliseconds,
    seconds,
    minutes,
    hours,
    days,
    weeks,
    months,
    quarters,
    years,
}

export function durationToMs(duration: number, unit: string): number {
    unit = `${unit}${unit.endsWith('s') ? '' : 's'}`
    if (typeof durations[unit] === 'undefined') {
        throw new Error(`Unknown time unit: ${unit}`)
    }
    return durations[unit] * duration
}

export function createJobs(server: Hub, pluginConfig: PluginConfig): Jobs {
    const runJob = async (type: string, payload: Record<string, any>, timestamp: number) => {
        try {
            const job = {
                type,
                payload,
                timestamp,
                pluginConfigId: pluginConfig.id,
                pluginConfigTeam: pluginConfig.team_id,
            }
            await server.jobQueueManager.enqueue(JobName.PLUGIN_JOB, job, {
                key: 'plugin',
                tag: pluginConfig.plugin?.name ?? '?',
            })
        } catch (e) {
            await pluginConfig.vm?.createLogEntry(
                `Failed to enqueue job ${type} with error: ${e.message}`,
                PluginLogEntryType.Error
            )
        }
    }

    return new Proxy(
        {},
        {
            get(target, key) {
                return function createTaskRunner(payload: Record<string, any>): JobRunner {
                    return {
                        runAt: async function runAt(date: Date) {
                            await runJob(key.toString(), payload, date.valueOf())
                        },
                        runIn: async function runIn(duration, unit) {
                            const timestamp = new Date().valueOf() + durationToMs(duration, unit)
                            await runJob(key.toString(), payload, timestamp)
                        },
                        runNow: async function runNow() {
                            await runJob(key.toString(), payload, new Date().valueOf())
                        },
                    }
                }
            },
        }
    )
}
