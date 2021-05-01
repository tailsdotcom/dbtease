"""Snowflake warehouse connection class."""

import logging
import snowflake.connector
import uuid
import click
from contextlib import contextmanager

from dbtease.warehouses.base import Sql, Warehouse

logger = logging.getLogger("dbtease.warehouses.snowflake")


class SnowflakeWarehouse(Warehouse):
    """Snowflake Connection."""

    # Locations for the state database
    state_database = "_dbtease_state"
    state_schema = "public"

    def __init__(self, user, password, account, warehouse, **kwargs):
        if "type" in kwargs:
            assert kwargs["type"] == "snowflake"
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        # self.database = database
        # self.schema = schema
        self._first_connect = True

    def _connect(self, autocommit=True):
        con = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            # Override to the default state store location.
            # TODO: Maybe do something more clever here?
            database=self.state_database,
            schema=self.state_schema,
            # For transactions
            autocommit=autocommit,
            session_parameters={
                'QUERY_TAG': 'dbtease',
            }
        )
        if self._first_connect:
            version = con.cursor().execute("select CURRENT_VERSION() as version").fetchone()
            if version:
                logger.info("Connected to snowflake. Snowflake version: %s", version[0])
            else:
                raise RuntimeError("Connection error when attempting to retrieve version from snowflake!")
            self._first_connect = False
        return con

    def _execute_sql(self, sql, params=None):
        con = self._connect(autocommit=True)
        logger.debug("Executing: %s", sql)
        if params and isinstance(sql, Sql) and sql.params:
            raise ValueError("Cannot use Sql.params and params in _execute_sql!")
        elif isinstance(sql, Sql):
            return con.cursor().execute(*sql).fetchall()
        elif params:
            return con.cursor().execute(sql, params).fetchall()
        else:
            return con.cursor().execute(sql).fetchall()

    def _execute_transaction(self, *statements):
        """Execute a series of statements in a transaction.

        https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-context-manager-to-connect-and-control-transactions
        """
        con = self._connect(autocommit=False)
        logger.debug("Starting Transaction...")
        try:
            for statement in statements:
                logger.debug("Executing: %s", statement)
                if isinstance(statement, Sql):
                    con.cursor().execute(*statement)
                else:
                    con.cursor().execute(statement)
            logger.debug("Committing Transaction...")
            con.commit()
        except Exception as e:
            logger.debug("Rolling Back Transaction...")
            con.rollback()
            raise e
        finally:
            con.close()

    def get_current_deployed(self, project_name):
        """Get the details of the currently deployed state."""
        try:
            current_live = self._execute_sql(
                "select commit_hash from live_deploys where project_name = %s",
                project_name
            )
        except snowflake.connector.errors.ProgrammingError:
            logger.warning(
                "Error fetching current live. Metadata store probably not yet initialised. "
                "This will happen on first deploy."
            )
            return None
        if current_live:
            return current_live[0][0]
        return None

    def deploy(self, project_name: str, commit_hash: str, build_db: str, deploy_db: str):
        """Deploy the current project."""
        # TODO: Make sure we acquire a lock first.
        self._execute_transaction(
            # Create table if not exists
            f"CREATE DATABASE IF NOT EXISTS {self.state_database}",
            f"CREATE SCHEMA IF NOT EXISTS {self.state_schema}",
            Sql(
                "CREATE TABLE IF NOT EXISTS live_deploys "
                " (project_name string, commit_hash string, manifest string)"
            ),
            # Do the upsert of new metadata
            Sql(
                """
                merge into live_deploys using (select %s as project_name, %s as commit_hash) as b
                        on live_deploys.project_name = b.project_name
                    when matched then update set live_deploys.commit_hash = b.commit_hash, live_deploys.manifest = NULL
                    when not matched then insert (project_name, commit_hash, manifest) values (b.project_name, b.commit_hash, NULL)
                """,
                (
                    project_name,
                    commit_hash,
                ),
            ),
            # Do the swap (creating the destination if it doesn't already exist).
            f"CREATE DATABASE IF NOT EXISTS {deploy_db}",
            f"ALTER DATABASE {build_db} SWAP WITH {deploy_db}",
            f"DROP DATABASE {build_db}",
        )
        logger.info("Deployed from %r to %r", build_db, deploy_db)

    def deploy_manifest(self, project_name: str, commit_hash: str, manifest: str):
        """update manifest for current project."""
        # Update manifest for this project
        self._execute_sql("UPDATE live_deploys SET manifest = %s WHERE project_name = %s and commit_hash = %s", (manifest, project_name, commit_hash))

    def acquire_lock(self, target: str, ttl_minutes=1):
        lock_key = str(uuid.uuid4())
        # Make sure we have a locks table.
        self._execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.state_database}")
        self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self.state_schema}")
        self._execute_sql(
            "CREATE TABLE IF NOT EXISTS database_locks "
            " (target_database string, process_id string, lock_timeout TIMESTAMP_NTZ)"
        )
        # Acquire lock if we can.
        self._execute_sql(
            """
            merge into database_locks using (
                        select
                            %s as target_database,
                            %s as process_id,
                            TIMESTAMPADD(
                                minute , %s , current_timestamp()
                            ) as lock_timeout
                    ) as b
                    on database_locks.target_database = b.target_database
                when matched and database_locks.lock_timeout < current_timestamp()
                    then update set database_locks.process_id = b.process_id, database_locks.lock_timeout = b.lock_timeout
                when not matched then insert (target_database, process_id, lock_timeout) values (b.target_database, b.process_id, b.lock_timeout)
            """,
            (target, lock_key, ttl_minutes)
        )
        # Did we get it?
        current_lock = self._execute_sql("SELECT process_id FROM database_locks WHERE target_database = %s", target)[0][0]
        if current_lock == lock_key:
            logger.info("Acquired lock on %r", target)
            return lock_key
        else:
            logger.info("Failed lock acquisition on %r", target)
            return None

    def create_wipe_db(self, db_name, source=None):
        if source:
            self._execute_sql(f"create or replace database {db_name} CLONE {source}")
        else:
            self._execute_sql(f"create or replace database {db_name}")

    def release_lock(self, target: str, lock_key:str):
        # SHOULD THIS BE A CONTEXT MANAGER?
        self._execute_sql("DELETE FROM database_locks WHERE target_database = %s and process_id = %s", (target, lock_key))
        logger.info("Lock released on %r", target)

    @contextmanager
    def lock(self, target: str, ttl_minutes=1):
        """Context Manager which implements acquire and release lock."""
        lock_key = self.acquire_lock(target=target, ttl_minutes=ttl_minutes)
        if not lock_key:
            raise click.ClickException(
                "Unable to lock {0!r}. Someone else has the lock. Try again later.".format(schedule.build_config["database"])
            )
        try:
            yield
        finally:
            self.release_lock(target, lock_key)
