"""Warehouse connections.

Eventually we should do something smart here to only load the
ones we can given the installed libraries.

That's a job for another day.
"""

from dbtease.warehouses.snowflake import SnowflakeWarehouse

_warehouse_options = {"snowflake": SnowflakeWarehouse}


def get_warehouse_from_target(target_dict):
    target_type = target_dict["type"]
    if target_type not in _warehouse_options:
        raise ValueError(
            f"Warehouses of type {target_type} are not supported yet in dbtease."
        )
    return _warehouse_options[target_type].from_target(target_dict)
