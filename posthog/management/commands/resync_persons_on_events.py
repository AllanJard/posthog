# based of 0006_persons_and_groups_on_events_backfill.py
# because we can't easily import sth that starts with a number :(
import importlib

import structlog
from django.conf import settings
from django.core.management.base import BaseCommand

from posthog.async_migrations.utils import execute_op_clickhouse

mig_0006_mod = importlib.import_module("posthog.async_migrations.migrations.0006_persons_and_groups_on_events_backfill")

logger = structlog.get_logger(__name__)


def run_resync(options):
    if not options["team_id"]:
        logger.error("You must specify --team-id to run this script")
        exit(1)

    TEAM_ID = options["team_id"]
    QUERY_ID = f"Persons on Events resync team {TEAM_ID}"

    # override to avoid instance lookup in get_parameter function
    class AsyncMig0006(mig_0006_mod.Migration):
        def get_parameter(self, name):
            return self.parameters[name][0]

    mig_0006 = AsyncMig0006(QUERY_ID)

    def run_step(id, sql, settings=None):
        logger.info(f"Step {id} running {sql}")
        execute_op_clickhouse(sql, query_id=QUERY_ID, per_shard=True, settings=settings)

    # start
    mig_0006._clear_temporary_tables(f"resync restart {TEAM_ID}")
    prep_steps = [
        # Create temporary tables  - copy/pasta from 0006
        f"""
            CREATE TABLE {mig_0006_mod.TEMPORARY_PERSONS_TABLE_NAME} {{on_cluster_clause}} AS {settings.CLICKHOUSE_DATABASE}.person
            ENGINE = ReplacingMergeTree(version)
            ORDER BY (team_id, id)
            SETTINGS index_granularity = 128 {mig_0006_mod.STORAGE_POLICY_SETTING()}
        """,
        f"""
            CREATE TABLE {mig_0006_mod.TEMPORARY_PDI2_TABLE_NAME} {{on_cluster_clause}} AS {settings.CLICKHOUSE_DATABASE}.person_distinct_id2
            ENGINE = ReplacingMergeTree(version)
            ORDER BY (team_id, distinct_id)
            SETTINGS index_granularity = 128
        """,
        f"""
            CREATE TABLE {mig_0006_mod.TEMPORARY_GROUPS_TABLE_NAME} {{on_cluster_clause}} AS {settings.CLICKHOUSE_DATABASE}.groups
            ENGINE = ReplacingMergeTree(_timestamp)
            ORDER BY (team_id, group_type_index, group_key)
            SETTINGS index_granularity = 128 {mig_0006_mod.STORAGE_POLICY_SETTING()}
        """,
        # fill them querying only for the selected team_id
        f"""
            INSERT INTO {mig_0006_mod.TEMPORARY_PERSONS_TABLE_NAME}(team_id, id, created_at, properties, is_deleted, version)
            SELECT
                team_id,
                id,
                created_at,
                properties,
                0 as is_deleted,
                ver as version
            FROM (
                SELECT
                    any(team_id) as team_id,
                    id,
                    argMax(created_at, version) as created_at,
                    argMax(properties, version) as properties,
                    max(version) as ver
                FROM
                    person
                WHERE
                    person.team_id = {TEAM_ID}
                GROUP BY
                    id
                HAVING
                    max(is_deleted) = 0
            )
        """,
        f"""
            INSERT INTO {mig_0006_mod.TEMPORARY_PDI2_TABLE_NAME}(team_id, distinct_id, person_id, is_deleted, version)
            SELECT
                team_id,
                distinct_id,
                person_id,
                0 as is_deleted,
                ver as version
            FROM (
                SELECT
                    distinct_id,
                    argMax(person_id, version) as person_id,
                    any(team_id) as team_id,
                    max(version) as ver
                FROM
                    person_distinct_id2
                WHERE
                    person_distinct_id2.team_id = {TEAM_ID}
                GROUP BY
                    distinct_id
                HAVING
                    max(is_deleted) = 0
            )
        """,
        # TODO: groups
    ]

    for id, step in enumerate(prep_steps):
        run_step(id, step)
    logger.info("Creating dictionaries")
    mig_0006._create_dictionaries(QUERY_ID)
    run_step(
        f"final alter for resync for {TEAM_ID}",
        mig_0006._alter_table_update_sql(f"WHERE team_id = {TEAM_ID}"),
        settings={"max_execution_time": 0},
    )
    mig_0006._wait_for_mutation_done(QUERY_ID)
    mig_0006._clear_temporary_tables(QUERY_ID)


class Command(BaseCommand):
    help = "Resync persons on events for a single team"

    def add_arguments(self, parser):
        parser.add_argument("--team-id", default=None, type=int, help="Specify a team to resync data for.")

    def handle(self, *args, **options):
        run_resync(options)
