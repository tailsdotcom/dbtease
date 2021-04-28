"""Defines repositories which store state."""

import json
import yaml
import os.path

import snowflake.connector

from dbtease.dbt import DbtProfiles


class DictStateRepository:
    """Basic state responsitory using an in memory dict."""

    def __init__(self):
        self._state = {}
    
    def _load_state(self):
        return self._state

    def _save_state(self, state):
        self._state = state

    def get_current_deployed(self, project, schedule):
        """Get the details of the currently deployed state."""
        state = self._load_state()
        if "current_deployed" not in state:
            return None
        return state["current_deployed"].get(schedule.name, None)

    def set_current_deployed(self, project, schedule, commit_hash):
        """Sets the details of the currently deployed state."""
        state = self._load_state()
        if "current_deployed" not in state:
            state["current_deployed"] = {}
        state["current_deployed"][schedule.name] = commit_hash
        self._save_state(state)


class JsonStateRepository(DictStateRepository):
    """Basic state responsitory using an in memory dict."""

    def __init__(self, fname="state.json"):
        self._fname = fname

    def _load_state(self):
        try:
            with open(self._fname) as state_file:
                return json.load(state_file)
        except FileNotFoundError:
            return {}

    def _save_state(self, state):
        with open(self._fname, "w") as state_file:
            return json.dump(state, state_file)


class SnowflakeStateRepository(DictStateRepository):
    """Basic state responsitory using an in memory dict."""

    def __init__(self, profiles_dir="~/.dbt/", state_database="_dbtease_state", state_schema="public"):
        # Expand use if necessary. This handles windows nicely.
        self.profiles_dir = os.path.expanduser(profiles_dir)
        self.state_database = state_database
        self.state_schema = state_schema
        self._profiles = {}
    
    def _load_profile(self, profile, target=None, profiles_file="profiles.yml"):
        # TODO: Probably needs much more exception handling.
        # TODO: Deal with jinja templating too.
        try:
            profiles = DbtProfiles.from_path(path=self.profiles_dir, profile=profile)
        except FileNotFoundError:
            return {}
        target_dict =  profiles.get_target_dict(target=target)
        assert target_dict["type"] == "snowflake"
        return target_dict
    
    def _get_profile(self, profile_name):
        if profile_name not in self._profiles:
            self._profiles[profile_name] = self._load_profile(profile=profile_name)
        return self._profiles[profile_name]
    
    def _sf_connection(self, profile_name, autocommit=True):
        # TODO: Do some nice stuff around importing snowflake and allowing
        # installation of the package without snowflake if someone is using
        # a different backend.
        profile = self._get_profile(profile_name)
        con = snowflake.connector.connect(
            user=profile["user"],
            password=profile["password"],
            account=profile["account"],
            warehouse=profile["warehouse"],
            database=self.state_database,
            schema=self.state_schema,
            # For transactions
            autocommit=autocommit,
            session_parameters={
                'QUERY_TAG': 'dbtease-metadata',
            }
        )
        version = con.cursor().execute("select CURRENT_VERSION() as version").fetchone()
        print("Connected to snowflake. Snowflake version: ", version)
        return con

    def get_current_deployed(self, project, schedule):
        """Get the details of the currently deployed state."""
        con = self._sf_connection(project.profile_name)
        try:
            current_live = con.cursor().execute("select commit_hash from live_deploys where project_name = %s", schedule.name).fetchone()
        except snowflake.connector.errors.ProgrammingError as e:
            print("Error fetching current live. Metadata store probably not set up...")
            print(e)
            return None
        # print("Current Live: ", current_live)
        # print(self._get_profile(profile_name=project.profile_name))
        if current_live:
            return current_live[0]
        return None

    def deploy(self, project, schedule, commit_hash: str, manifest: str, build_db: str, deploy_db: str):
        """Deploy the current project."""
        # NOTE: TODO - in transactions, merge results and swap DB
        # TODO: Make sure we acquire a lock first.
        # https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-context-manager-to-connect-and-control-transactions
        con = self._sf_connection(project.profile_name, autocommit=False)
        try:
            # Create table if not exists
            # TODO: Handle persmissions somewhere. Probably Docs.
            con.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.state_database}")
            con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {self.state_schema}")
            con.cursor().execute(
                "CREATE TABLE IF NOT EXISTS live_deploys "
                " (project_name string, commit_hash string, manifest string)"
            )
            # Do the upsert of new metadata
            con.cursor().execute(
                (
                    "merge into live_deploys using (select %s as project_name, %s as commit_hash, %s as manifest) as b "
                    "on live_deploys.project_name = b.project_name "
                    "when matched then update set live_deploys.commit_hash = b.commit_hash, live_deploys.manifest = b.manifest "
                    "when not matched then insert (project_name, commit_hash, manifest) values (b.project_name, b.commit_hash, b.manifest)"
                ),
                (
                    schedule.name,
                    commit_hash,
                    manifest,
                ),
            )
            # Do the swap (creating the destination if it doesn't already exist).
            con.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {deploy_db}")
            con.cursor().execute(f"ALTER DATABASE {build_db} SWAP WITH {deploy_db}")
            # Remove the old database now that we're done.
            con.cursor().execute(f"DROP DATABASE {build_db}")
            # And... commit
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            con.close()

    def set_current_deployed(self, project, schedule, commit_hash, manifest):
        """Sets the details of the currently deployed state."""
        con = self._sf_connection(project.profile_name)
        # Create table if not exists
        # TODO: Handle persmissions somewhere. Probably Docs.
        con.cursor().execute("CREATE DATABASE IF NOT EXISTS {}".format(self.state_database))
        con.cursor().execute("CREATE SCHEMA IF NOT EXISTS {}".format(self.state_schema))
        con.cursor().execute(
            "CREATE TABLE IF NOT EXISTS live_deploys "
            " (project_name string, commit_hash string, manifest string)"
        )
        # Do the upsert
        con.cursor().execute(
            (
                "merge into live_deploys using (select %s as project_name, %s as commit_hash, %s as manifest) as b "
                "on live_deploys.project_name = b.project_name "
                "when matched then update set live_deploys.commit_hash = b.commit_hash, live_deploys.manifest = b.manifest"
                "when not matched then insert (project_name, commit_hash, manifest) values (b.project_name, b.commit_hash, b.manifest)"
            ),
            (
                schedule.name,
                commit_hash,
                manifest,
            ),
        )
