"""Routines for loading the dbt_schedule.yml file."""

import yaml
import os.path
import networkx as nx

from dbtease.schema import DbtSchema
from dbtease.repository import JsonStateRepository, SnowflakeStateRepository
from dbtease.git import get_git_state
from dbtease.common import YamlFileObject


class NotDagException(ValueError):
    pass


class DbtSchedule(YamlFileObject):
    """A schedule for dbt."""

    default_file_name = "dbt_schedule.yml"

    def __init__(self, name, graph, state_repository, git_path=".", build_config=None, deploy_config=None):
        self.name = name
        self.graph = graph
        self.state_repository = state_repository
        self.git_path = git_path
        self.build_config = build_config or {}
        self.deploy_config = deploy_config or {}

    def iter_schemas(self):
        for node_name in self.graph.nodes:
            yield node_name, self.graph.nodes[node_name]["schema"]

    def _iter_affected_schemas(self, paths):
        for _, schema in self.iter_schemas():
            matched_paths = schema.matches_paths(paths)
            if matched_paths:
                yield schema, matched_paths

    def _match_changed_files(self, changed_files):
        matched_files = set()
        schema_files = {}
        for schema, files in self._iter_affected_schemas(paths=changed_files):
            matched_files |= files
            schema_files[schema.name] = files
        unmatched_files = changed_files - matched_files
        return schema_files, unmatched_files

    def _get_dependent_schemas(self, *changed_schemas):
        dependent_schemas = set()
        for changed_schema in changed_schemas:
            dependent_schemas |= nx.algorithms.dag.descendants(
                self.graph,
                changed_schema
            )
        return dependent_schemas

    def materialized_schemas(self):
        return set(
            name for name, schema in self.iter_schemas()
            if schema.materialized
        )

    def _plan_from_changed_files(self, changed_files):
        """Generate a plan of attack from changed files."""
        schema_files, unmatched_files = self._match_changed_files(
            changed_files
        )
        changed_schemas = {*schema_files.keys()}
        # Filter only to materialized schemas using set operators
        deploy_schemas = (
            self._get_dependent_schemas(*changed_schemas)
            & self.materialized_schemas()
        )
        # Lastly, for the changed and dependent schemas, we need to
        # identify an appropriate order of operations.
        # We do this by iterating a sorted generator on the graph.
        # NOTE: This is not necessarily deterministic in the case
        # that nodes have the same level in the tree, but I don't think
        # that really matters at this stage.
        deploy_order = []
        for node in nx.topological_sort(self.graph):
            if node in changed_schemas or node in deploy_schemas:
                deploy_order.append(node)
        return {
            "unmatched_files": unmatched_files,
            "matched_files": schema_files,
            "changed_schemas": changed_schemas,
            "dependent_deploy_schemas": deploy_schemas,
            "deploy_order": deploy_order
        }

    def status_dict(self, project, project_dir=None):
        """Determine the current status of the repository."""
        # Load state
        deployed_hash = self.state_repository.get_current_deployed(project, schedule=self)
        # Introspect git status
        git_status = get_git_state(deployed_hash=deployed_hash, repo_dir=self.git_path)
        # Make a plan from the changed files
        changed_files = git_status["diff"] | git_status["untracked"]
        # Adjust for project dir if we need to.
        if project_dir:
            changed_files = {
                os.path.relpath(fpath, project_dir)
                for fpath in changed_files
                if os.path.abspath(fpath).startswith(os.path.abspath(project_dir))
            }
        plan_dict = self._plan_from_changed_files(changed_files)
        return {
            "deployed_hash": deployed_hash,
            "current_hash": git_status["commit_hash"],
            "dirty_tree": git_status["dirty"],
            "changed_files": changed_files,
            **plan_dict
        }

    @classmethod
    def from_dict(cls, config, state_repository=None, **kwargs):
        """Load a schedule from a dict."""
        # Set up the graph
        dag = nx.DiGraph()
        for name, schema_config in config["schemas"].items():
            schema = DbtSchema.from_dict(name=name, config=schema_config)
            dag.add_node(name, schema=schema)
            if "depends_on" in schema_config:
                dag.add_edges_from([
                    (s, name) for s in schema_config["depends_on"]
                ])
        if not nx.algorithms.dag.is_directed_acyclic_graph(dag):
            raise NotDagException("Not a DAG!")
        # Set up the state repository:
        if not state_repository:
            if "state" not in config:
                raise ValueError("No repository config found!")
            engine = config["state"].pop("engine", None)
            if not engine:
                raise ValueError("No repository state engine found!")
            state_config = config["state"]
            for state_repo_kwarg in ["profiles_dir"]:
                if state_repo_kwarg in kwargs:
                    state_config[state_repo_kwarg] = kwargs[state_repo_kwarg]
            state_repository = {
                "json": JsonStateRepository,
                "snowflake": SnowflakeStateRepository,
            }[engine](**state_config)
        
        # Config kwargs
        schedule_kwargs = {
            "name": config["deployment"],
            "graph": dag,
            "state_repository": state_repository
        }
        # Use the git path if provided.
        if "git_path" in config:
            schedule_kwargs["git_path"] = config["git_path"]

        # Add build and deployy configs if present.
        if "deploy" in config:
            schedule_kwargs["deploy_config"] = config["deploy"]
        if "build" in config:
            schedule_kwargs["build_config"] = config["build"]
        return cls(**schedule_kwargs)
