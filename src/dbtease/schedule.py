"""Routines for loading the dbt_schedule.yml file."""

import yaml
import os.path
import networkx as nx

from dbtease.schema import DbtSchema


class DbtSchedule:
    """A schedule for dbt."""

    def __init__(self, name, graph):
        self.name = name
        self.graph = graph
    
    def iter_schemas(self):
        for node_name in self.graph.nodes:
            yield node_name, self.graph.nodes[node_name]["schema"]

    def iter_affected_schemas(self, paths):
        for _, schema in self.iter_schemas():
            matched_paths = schema.matches_paths(paths)
            if matched_paths:
                yield schema, matched_paths

    def match_changed_files(self, changed_files):
        matched_files = set()
        schema_files = {}
        for schema, files in self.iter_affected_schemas(paths=changed_files):
            matched_files |= files
            schema_files[schema.name] = files
        unmatched_files = changed_files - matched_files
        return schema_files, unmatched_files

    @classmethod
    def from_dict(cls, config):
        """Load a schedule from a dict."""
        dag = nx.DiGraph()
        for name, schema_config in config["schemas"].items():
            schema = DbtSchema.from_dict(name=name, config=schema_config)
            dag.add_node(name, schema=schema)
            if "depends_on" in schema_config:
                dag.add_edges_from([
                    (s, name) for s in schema_config["depends_on"]
                ])
        return cls(name=config["deployment"], graph=dag)

    @classmethod
    def from_file(cls, fname):
        """Load a schedule from a file."""
        with open(fname) as schedule_file:
            config_dict = yaml.safe_load(schedule_file.read())
        return cls.from_dict(config_dict)

    @classmethod
    def from_path(cls, path, fname="dbt_schedule.yml"):
        """Load a schedule from a path."""
        return cls.from_file(fname=os.path.join(path, fname))
