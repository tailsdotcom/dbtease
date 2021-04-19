"""Test the schedule module."""

from dbtease.schedule import DbtSchedule


def test_load_basic():
    schedule = DbtSchedule.from_path("test/fixtures")
    # Make sure we've got the right name
    assert schedule.name == "foo_prod"
    # Make sure we've got the edges we expect.
    assert set(schedule.graph.edges) == {
        ('base', 'mid'),
        ('base', 'upper_a'),
        ('base', 'upper_b'),
        ('base', 'top'),
        ('mid', 'upper_a'),
        ('mid', 'upper_b'),
        ('mid', 'top'),
        ('upper_a', 'top'),
        ('upper_b', 'top'),
    }
