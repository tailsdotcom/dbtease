"""Snowflake warehouse connection class."""

import logging
import snowflake.connector

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
        version = con.cursor().execute("select CURRENT_VERSION() as version").fetchone()
        logger.info("Connected to snowflake. Snowflake version: %s", version)
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

    def deploy(self, project_name: str, commit_hash: str, manifest: str, build_db: str, deploy_db: str):
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
                (
                    "merge into live_deploys using (select %s as project_name, %s as commit_hash, %s as manifest) as b "
                    "on live_deploys.project_name = b.project_name "
                    "when matched then update set live_deploys.commit_hash = b.commit_hash, live_deploys.manifest = b.manifest "
                    "when not matched then insert (project_name, commit_hash, manifest) values (b.project_name, b.commit_hash, b.manifest)"
                ),
                (
                    project_name,
                    commit_hash,
                    manifest,
                ),
            ),
            # Do the swap (creating the destination if it doesn't already exist).
            f"CREATE DATABASE IF NOT EXISTS {deploy_db}",
            f"ALTER DATABASE {build_db} SWAP WITH {deploy_db}",
            f"DROP DATABASE {build_db}",
        )
