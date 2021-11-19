from datetime import datetime

from ee.clickhouse.queries.funnels.funnel_strict import ClickhouseFunnelStrict
from ee.clickhouse.queries.funnels.funnel_strict_persons import ClickhouseFunnelStrictPersons
from ee.clickhouse.queries.funnels.test.breakdown_cases import (
    assert_funnel_results_equal,
    funnel_breakdown_test_factory,
)
from ee.clickhouse.queries.funnels.test.conversion_time_cases import funnel_conversion_time_test_factory
from ee.clickhouse.test.test_journeys import journeys_for
from ee.clickhouse.util import ClickhouseTestMixin
from posthog.constants import INSIGHT_FUNNELS
from posthog.models import Person
from posthog.models.action import Action
from posthog.models.action_step import ActionStep
from posthog.models.filters import Filter
from posthog.test.base import APIBaseTest

FORMAT_TIME = "%Y-%m-%d 00:00:00"


def _create_action(**kwargs):
    team = kwargs.pop("team")
    name = kwargs.pop("name")
    properties = kwargs.pop("properties", {})
    action = Action.objects.create(team=team, name=name)
    ActionStep.objects.create(action=action, event=name, properties=properties)
    return action


def _create_person(**kwargs):
    person = Person.objects.create(**kwargs)
    return Person(id=person.uuid, uuid=person.uuid)


class TestFunnelStrictStepsBreakdown(ClickhouseTestMixin, funnel_breakdown_test_factory(ClickhouseFunnelStrict, ClickhouseFunnelStrictPersons, _create_action, _create_person)):  # type: ignore

    maxDiff = None

    def test_strict_breakdown_events_with_multiple_properties(self):

        filters = {
            "events": [{"id": "sign up", "order": 0}, {"id": "play movie", "order": 1}],
            "insight": INSIGHT_FUNNELS,
            "date_from": "2020-01-01",
            "date_to": "2020-01-08",
            "funnel_window_days": 7,
            "breakdown_type": "event",
            "breakdown": "$browser",
        }

        filter = Filter(data=filters)
        funnel = ClickhouseFunnelStrict(filter, self.team)

        people = journeys_for(
            {
                "person1": [
                    {"event": "sign up", "timestamp": datetime(2020, 1, 1, 12), "properties": {"$browser": "Chrome"}},
                    {"event": "blah", "timestamp": datetime(2020, 1, 1, 13), "properties": {"$browser": "Chrome"}},
                    {
                        "event": "play movie",
                        "timestamp": datetime(2020, 1, 1, 14),
                        "properties": {"$browser": "Chrome"},
                    },
                ],
                "person2": [
                    {"event": "sign up", "timestamp": datetime(2020, 1, 2, 13), "properties": {"$browser": "Safari"}},
                    {
                        "event": "play movie",
                        "timestamp": datetime(2020, 1, 2, 14),
                        "properties": {"$browser": "Safari"},
                    },
                ],
            },
            self.team,
        )

        result = funnel.run()

        assert_funnel_results_equal(
            result[0],
            [
                {
                    "action_id": "sign up",
                    "name": "sign up",
                    "custom_name": None,
                    "order": 0,
                    "people": [],
                    "count": 1,
                    "type": "events",
                    "average_conversion_time": None,
                    "median_conversion_time": None,
                    "breakdown": "Chrome",
                    "breakdown_value": "Chrome",
                },
                {
                    "action_id": "play movie",
                    "name": "play movie",
                    "custom_name": None,
                    "order": 1,
                    "people": [],
                    "count": 0,
                    "type": "events",
                    "average_conversion_time": None,
                    "median_conversion_time": None,
                    "breakdown": "Chrome",
                    "breakdown_value": "Chrome",
                },
            ],
        )
        self.assertCountEqual(self._get_people_at_step(filter, 1, "Chrome"), [people["person1"].uuid])
        self.assertCountEqual(self._get_people_at_step(filter, 2, "Chrome"), [])

        assert_funnel_results_equal(
            result[1],
            [
                {
                    "action_id": "sign up",
                    "name": "sign up",
                    "custom_name": None,
                    "order": 0,
                    "people": [],
                    "count": 1,
                    "type": "events",
                    "average_conversion_time": None,
                    "median_conversion_time": None,
                    "breakdown": "Safari",
                    "breakdown_value": "Safari",
                },
                {
                    "action_id": "play movie",
                    "name": "play movie",
                    "custom_name": None,
                    "order": 1,
                    "people": [],
                    "count": 1,
                    "type": "events",
                    "average_conversion_time": 3600,
                    "median_conversion_time": 3600,
                    "breakdown": "Safari",
                    "breakdown_value": "Safari",
                },
            ],
        )
        self.assertCountEqual(self._get_people_at_step(filter, 1, "Safari"), [people["person2"].uuid])
        self.assertCountEqual(self._get_people_at_step(filter, 2, "Safari"), [people["person2"].uuid])


class TestFunnelStrictStepsConversionTime(ClickhouseTestMixin, funnel_conversion_time_test_factory(ClickhouseFunnelStrict, ClickhouseFunnelStrictPersons)):  # type: ignore

    maxDiff = None
    pass


class TestFunnelStrictSteps(ClickhouseTestMixin, APIBaseTest):

    maxDiff = None

    def _get_people_at_step(self, filter, funnel_step, breakdown_value=None):
        person_filter = filter.with_data({"funnel_step": funnel_step, "funnel_step_breakdown": breakdown_value})
        result = ClickhouseFunnelStrictPersons(person_filter, self.team)._exec_query()
        return [row[0] for row in result]

    def test_basic_strict_funnel(self):
        filter = Filter(
            data={
                "insight": INSIGHT_FUNNELS,
                "events": [
                    {"id": "user signed up", "order": 0},
                    {"id": "$pageview", "order": 1},
                    {"id": "insight viewed", "order": 2},
                ],
            }
        )

        funnel = ClickhouseFunnelStrict(filter, self.team)

        people = journeys_for(
            {
                "stopped_after_signup1": [{"event": "user signed up"},],
                "stopped_after_pageview1": [{"event": "$pageview"}, {"event": "user signed up"},],
                "stopped_after_insightview": [
                    {"event": "user signed up"},
                    {"event": "$pageview"},
                    {"event": "blaah blaa"},
                    {"event": "insight viewed"},
                ],
                "stopped_after_insightview2": [
                    {"event": "insight viewed"},
                    {"event": "blaah blaa"},
                    {"event": "$pageview"},
                    {"event": "user signed up"},
                ],
                "stopped_after_insightview3": [
                    {"event": "$pageview"},
                    {"event": "user signed up"},
                    {"event": "blaah blaa"},
                    {"event": "$pageview"},
                    {"event": "insight viewed"},
                ],
                "person6": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "blaah blaa"},
                    {"event": "$pageview"},
                ],
                "person7": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "$pageview"},
                    {"event": "insight viewed"},
                    {"event": "blaah blaa"},
                ],
                "stopped_after_insightview6": [{"event": "insight viewed"}, {"event": "$pageview"},],
            },
            self.team,
        )

        result = funnel.run()

        self.assertEqual(result[0]["name"], "user signed up")
        self.assertEqual(result[1]["name"], "$pageview")
        self.assertEqual(result[2]["name"], "insight viewed")
        self.assertEqual(result[0]["count"], 7)

        self.assertCountEqual(
            self._get_people_at_step(filter, 1),
            [
                people["stopped_after_signup1"].uuid,
                people["stopped_after_pageview1"].uuid,
                people["stopped_after_insightview"].uuid,
                people["stopped_after_insightview2"].uuid,
                people["stopped_after_insightview3"].uuid,
                people["person6"].uuid,
                people["person7"].uuid,
            ],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 2), [people["stopped_after_insightview"].uuid, people["person7"].uuid,],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 3), [people["person7"].uuid,],
        )

    def test_advanced_strict_funnel(self):

        sign_up_action = _create_action(
            name="sign up",
            team=self.team,
            properties=[{"key": "key", "type": "event", "value": ["val"], "operator": "exact"}],
        )

        view_action = _create_action(
            name="pageview",
            team=self.team,
            properties=[{"key": "key", "type": "event", "value": ["val"], "operator": "exact"}],
        )

        filters = {
            "events": [
                {"id": "user signed up", "type": "events", "order": 0},
                {"id": "$pageview", "type": "events", "order": 2},
            ],
            "actions": [
                {"id": sign_up_action.id, "math": "dau", "order": 1},
                {"id": view_action.id, "math": "wau", "order": 3},
            ],
            "insight": INSIGHT_FUNNELS,
        }

        filter = Filter(data=filters)
        funnel = ClickhouseFunnelStrict(filter, self.team)

        people = journeys_for(
            {
                "stopped_after_signup1": [{"event": "user signed up"}],
                "stopped_after_pageview1": [{"event": "user signed up"}, {"event": "$pageview"},],
                "stopped_after_insightview": [
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "sign up", "properties": {"key": "val2"}},
                    {"event": "$pageview"},
                    {"event": "blaah blaa"},
                    {"event": "insight viewed"},
                ],
                "person4": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "$pageview", "properties": {"key": "val"}},
                    {"event": "blaah blaa"},
                ],
                "person5": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "$pageview"},
                    {"event": "blaah blaa"},
                ],
                "person6": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "$pageview"},
                    {"event": "pageview", "properties": {"key": "val1"}},
                ],
                "person7": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "$pageview"},
                    {"event": "user signed up"},
                    {"event": "pageview", "properties": {"key": "val"}},
                ],
                "person8": [
                    {"event": "blaah blaa"},
                    {"event": "user signed up"},
                    {"event": "user signed up"},
                    {"event": "sign up", "properties": {"key": "val"}},
                    {"event": "$pageview"},
                    {"event": "pageview", "properties": {"key": "val"}},
                ],
            },
            self.team,
        )

        result = funnel.run()

        self.assertEqual(result[0]["name"], "user signed up")
        self.assertEqual(result[1]["name"], "sign up")
        self.assertEqual(result[2]["name"], "$pageview")
        self.assertEqual(result[3]["name"], "pageview")
        self.assertEqual(result[0]["count"], 8)

        self.assertCountEqual(
            self._get_people_at_step(filter, 1),
            [
                people["stopped_after_signup1"].uuid,
                people["stopped_after_pageview1"].uuid,
                people["stopped_after_insightview"].uuid,
                people["person4"].uuid,
                people["person5"].uuid,
                people["person6"].uuid,
                people["person7"].uuid,
                people["person8"].uuid,
            ],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 2),
            [
                people["stopped_after_insightview"].uuid,
                people["person4"].uuid,
                people["person5"].uuid,
                people["person6"].uuid,
                people["person7"].uuid,
                people["person8"].uuid,
            ],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 3),
            [
                people["person4"].uuid,
                people["person5"].uuid,
                people["person6"].uuid,
                people["person7"].uuid,
                people["person8"].uuid,
            ],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 4), [people["person8"].uuid,],
        )

    def test_basic_strict_funnel_conversion_times(self):
        filter = Filter(
            data={
                "insight": INSIGHT_FUNNELS,
                "events": [
                    {"id": "user signed up", "order": 0},
                    {"id": "$pageview", "order": 1},
                    {"id": "insight viewed", "order": 2},
                ],
                "date_from": "2021-05-01 00:00:00",
                "date_to": "2021-05-07 23:59:59",
            }
        )

        funnel = ClickhouseFunnelStrict(filter, self.team)

        people = journeys_for(
            {
                "stopped_after_signup1": [{"event": "user signed up", "timestamp": datetime(2021, 5, 2)}],
                "stopped_after_pageview1": [
                    {"event": "user signed up", "timestamp": datetime(2021, 5, 2)},
                    {"event": "$pageview", "timestamp": datetime(2021, 5, 2, 1)},
                ],
                "stopped_after_insightview": [
                    {"event": "user signed up", "timestamp": datetime(2021, 5, 2)},
                    {"event": "$pageview", "timestamp": datetime(2021, 5, 2, 2)},
                    {"event": "insight viewed", "timestamp": datetime(2021, 5, 2, 4)},
                ],
            },
            self.team,
        )

        result = funnel.run()

        self.assertEqual(result[0]["name"], "user signed up")
        self.assertEqual(result[1]["name"], "$pageview")
        self.assertEqual(result[2]["name"], "insight viewed")
        self.assertEqual(result[0]["count"], 3)

        self.assertEqual(result[1]["average_conversion_time"], 5400)
        # 1 hour for Person 2, 2 hours for Person 3, average = 1.5 hours

        self.assertEqual(result[2]["average_conversion_time"], 7200)
        # 2 hours for Person 3

        self.assertCountEqual(
            self._get_people_at_step(filter, 1),
            [
                people["stopped_after_signup1"].uuid,
                people["stopped_after_pageview1"].uuid,
                people["stopped_after_insightview"].uuid,
            ],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 2),
            [people["stopped_after_pageview1"].uuid, people["stopped_after_insightview"].uuid,],
        )

        self.assertCountEqual(
            self._get_people_at_step(filter, 3), [people["stopped_after_insightview"].uuid,],
        )
