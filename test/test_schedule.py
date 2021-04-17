"""Test the schedule module."""

from bernie.schedule import DbtSchedule

def test_load_basic():
    schedule = DbtSchedule.from_path("test/fixtures")
    print(schedule)
    print(config)
    print(schedule.graph)
    print(schedule.graph.nodes)
    print(schedule.graph.edges)
    assert schedule.graph.edges == [
        ('base', 'mid'),
        ('base', 'upper_a'),
        ('base', 'upper_b'),
        ('base', 'top'),
        ('mid', 'upper_a'),
        ('mid', 'upper_b'),
        ('mid', 'top'),
        ('upper_a', 'top'),
        ('upper_b', 'top')
    ]
    assert False
