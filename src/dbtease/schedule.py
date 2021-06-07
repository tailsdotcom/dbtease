"""Routines for loading the dbt_schedule.yml file."""

import networkx as nx
import logging
import click

from dbtease.schema import DbtSchema
from dbtease.warehouses import get_warehouse_from_target
from dbtease.dbt import DbtProfiles, DbtProject
from dbtease.git import get_git_state
from dbtease.common import YamlFileObject
from dbtease.filestores import get_filestore_from_config
from dbtease.cron import refresh_due
from dbtease.alerts import AlterterBundle

logger = logging.getLogger("dbtease.schedule")


class NotDagException(ValueError):
    pass


class DbtSchedule(YamlFileObject):
    """A schedule for dbt."""

    default_file_name = "dbt_schedule.yml"

    def __init__(
        self,
        name,
        graph,
        warehouse,
        project,
        project_dir=".",
        git_path=".",
        build_config=None,
        deploy_config=None,
        filestore=None,
        redeploy_schedule=None,
        alerter_bundle=None,
        schema_prefix=None,
    ):
        self.name = name
        self.graph = graph
        self.warehouse = warehouse
        self.project = project
        self.git_path = git_path
        self.project_dir = project_dir
        self.build_config = build_config or {}
        self.deploy_config = deploy_config or {}
        self.filestore = filestore
        self.redeploy_schedule = redeploy_schedule
        self.alerter_bundle = alerter_bundle
        self.schema_prefix = schema_prefix

    def handle_event(
        self, alert_event: str, success: bool, message: str, metadata=None
    ):
        """Broadcast this event to the alert bundler if present."""
        if self.alerter_bundle:
            self.alerter_bundle.handle_event(
                alert_event=alert_event,
                success=success,
                message=message,
                metadata=metadata,
            )

    def get_schema(self, schema):
        try:
            return self.graph.nodes[schema]["schema"]
        except KeyError:
            raise click.ClickException(
                f"Schema {schema!r} is referred to but is not defined."
            )

    def iter_schemas(self):
        for node_name in self.graph.nodes:
            yield node_name, self.get_schema(node_name)

    def _iter_affected_schemas(self, paths):
        for _, schema in self.iter_schemas():
            matched_paths = schema.matches_paths(paths)
            if matched_paths:
                yield schema, matched_paths

    def _match_changed_files(self, changed_files):
        changed_files = set(changed_files)
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
                self.graph, changed_schema
            )
        return dependent_schemas

    def materialized_schemas(self):
        return set(name for name, schema in self.iter_schemas() if schema.materialized)

    def _determine_deploy_order(self, schemas):
        deploy_order = []
        for node in nx.topological_sort(self.graph):
            if node in schemas:
                deploy_order.append(node)
        return deploy_order

    def _plan_from_changed_files(self, changed_files, deploy=True):
        """Generate a plan of attack from changed files."""
        schema_files, unmatched_files = self._match_changed_files(changed_files)
        changed_schemas = {*schema_files.keys()}
        # Filter only to materialized schemas using set operators
        deploy_schemas = self._get_dependent_schemas(*changed_schemas)
        # When refreshing, we don't need to refresh view schemas
        if not deploy:
            deploy_schemas &= self.materialized_schemas()
        # Lastly, for the changed and dependent schemas, we need to
        # identify an appropriate order of operations.
        # We do this by iterating a sorted generator on the graph.
        # NOTE: This is not necessarily deterministic in the case
        # that nodes have the same level in the tree, but I don't think
        # that really matters at this stage.
        matched_schemas = changed_schemas | deploy_schemas
        return {
            "unmatched_files": unmatched_files,
            "matched_files": schema_files,
            "changed_schemas": changed_schemas,
            "dependent_deploy_schemas": deploy_schemas,
            "deploy_order": self._determine_deploy_order(matched_schemas),
            "trigger_full_deploy": any(
                self.get_schema(sch).triggers_full_deploy for sch in matched_schemas
            ),
        }

    def redeploy_due(self, last_refresh):
        """Work out whether a refresh is due based on cron and last refresh."""
        # Only redeploy at all if this is configured.
        if not self.redeploy_schedule:
            return False
        return refresh_due(self.redeploy_schedule, last_refresh)

    def evaluate_schedules(self):
        last_refreshes = self.warehouse.get_last_refreshes(self.name)
        last_redeploy = last_refreshes.get(self.warehouse.FULL_DEPLOY, None)
        refresh_due_schemas = [
            schema_name
            for schema_name, schema in self.iter_schemas()
            if schema.refresh_due(last_refreshes.get(schema_name, None))
        ]
        return {
            "redeploy_due": self.redeploy_due(last_redeploy),
            "refreshes_due": self._determine_deploy_order(refresh_due_schemas),
        }

    def status_dict(self, deploy=True):
        """Determine the current status of the repository."""
        # Load state
        deployed_hash = self.warehouse.get_current_deployed(self.name)
        # Evaluate refreshes due
        refreshes_due = self.evaluate_schedules()
        # Introspect git status
        git_status = get_git_state(repo_dir=self.git_path)
        return {
            "deployed_hash": deployed_hash,
            "current_hash": git_status["commit_hash"],
            "dirty_tree": git_status["dirty"],
            **refreshes_due,
        }

    def generate_plan_from_paths(self, changed_files, deploy=True):
        """From differing paths, determine a plan."""
        # Adjust for project dir if we need to.
        return self._plan_from_changed_files(changed_files, deploy=deploy)

    @classmethod
    def from_dict(
        cls,
        config,
        warehouse=None,
        target_dict=None,
        project=None,
        project_dir=".",
        target_name=None,
        filestore=None,
        profiles_dir=None,
        aws_profile=None,
        **kwargs,
    ):
        """Load a schedule from a dict."""
        # Set up the graph
        dag = nx.DiGraph()
        for name, schema_config in config["schemas"].items():
            schema = DbtSchema.from_dict(name=name, config=schema_config)
            dag.add_node(name, schema=schema)
            if "depends_on" in schema_config:
                dag.add_edges_from([(s, name) for s in schema_config["depends_on"]])
        if not nx.algorithms.dag.is_directed_acyclic_graph(dag):
            raise NotDagException("Not a DAG!")

        # First precedence is override, then file config, then default.
        profiles_dir = (
            profiles_dir or kwargs.get("dbt_profiles_path", None) or "~/.dbt/"
        )

        # Make sure we've got a project
        if not project:
            # Load project
            project = DbtProject.from_path(project_dir, profiles_dir=profiles_dir)

        # Set up the state warehouse connection:
        if not warehouse:
            # Get the details of the target from the profiles file if not provided.
            if not target_dict:
                # TODO: Probably needs much more exception handling.
                # TODO: Deal with jinja templating too.
                profiles = DbtProfiles.from_path(
                    path=profiles_dir, profile=project.profile_name
                )
                target_dict = profiles.get_target_dict(target=target_name)

            warehouse = get_warehouse_from_target(target_dict)

        if not filestore:
            if "docs" in config:
                filestore = get_filestore_from_config(
                    config["docs"], aws_profile=aws_profile
                )

        # Config kwargs
        schedule_kwargs = {
            "name": config["deployment"],
            "graph": dag,
            "warehouse": warehouse,
            "project": project,
            "project_dir": project_dir,
            "filestore": filestore,
        }
        # Use the git path if provided.
        if "git_path" in config:
            schedule_kwargs["git_path"] = config["git_path"]

        # Use schema prefix if provided.
        if "schema_prefix" in config:
            schedule_kwargs["schema_prefix"] = config["schema_prefix"]

        # Add build and deploy configs if present.
        if "deploy" in config:
            schedule_kwargs["deploy_config"] = config["deploy"]
        if "build" in config:
            schedule_kwargs["build_config"] = config["build"]

        # Add redeploy schedule if present
        if "redeploy_schedule" in config:
            schedule_kwargs["redeploy_schedule"] = config["redeploy_schedule"]

        # Add redeploy schedule if present
        if "alert" in config:
            schedule_kwargs["alerter_bundle"] = AlterterBundle.from_config(
                config["alert"]
            )

        return cls(**schedule_kwargs)
