import click
import logging
import sys

from dbtease.schedule import DbtSchedule
from dbtease.dbt import DbtProject

from dbtease.config_context import ConfigContext
from dbtease.shell import run_shell_command


# Set up logging properly
root = logging.getLogger("dbtease")
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


@click.group()
@click.version_option()
def cli():
    pass


def common_setup(project_dir, profiles_dir, schedule_dir):
    schedule_dir = schedule_dir or project_dir
    # Load the schedule
    schedule = DbtSchedule.from_path(schedule_dir, profiles_dir=profiles_dir, project_dir=project_dir)
    status_dict = schedule.status_dict()
    return schedule, status_dict


@cli.command()
@click.option('--project-dir', default=".")
@click.option('--profiles-dir', default="~/.dbt/")
@click.option('--schedule-dir', default=None)
def status(project_dir, profiles_dir, schedule_dir):
    """Get the current status of deployment."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Output the status.
    click.echo("=== dbtease status ===")
    config_pairs = [
        ("Deployment Name", schedule.name),
        ("Deployed Hash", status_dict["deployed_hash"]),
        ("Current Hash", status_dict["current_hash"]),
        ("Uncommitted Changes", status_dict["dirty_tree"]),
        ("Deployment Plan", ", ".join(status_dict["deploy_order"])),
    ]
    for label, value in config_pairs:
        click.echo(f"{label:22} - {value}")
    # Output Files affected
    if status_dict["unmatched_files"]:
        click.echo("== non-project file changes ==")
        for fname in status_dict["unmatched_files"]:
            click.echo(f"- {fname}")
    for schema_name in status_dict["matched_files"]:
        click.echo(f"== schema: {schema_name} ==")
        for fname in status_dict["matched_files"][schema_name]:
            click.echo(f"- {fname}")


@cli.command()
def test():
    """Tests the current active changes."""
    # Load project
    project = DbtProject.from_path(".")
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    status_dict = schedule.status_dict(project=project)
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if deployed_hash == current_hash and not status_dict["dirty_tree"]:
        raise click.UsageError(
            "No changes made compared to deployed version. "
            "To build regardless, call `dbt run` directly."
        )

    if not deployed_hash:
        click.secho(
            (
                "WARNING: No currently deployed hash, this will mean "
                "a full test build. In a large project this "
                "may take some time..."
            ),
            fg='yellow'
        )
        defer_to_state = False
    else:
        defer_to_state = True

    # Always use --fail-fast
    plan = [
        "acquire lock on build database",  # using generated name if we're in codeship?
        "seed",  # use state:modified if we can.
        "snapshot",  # currently this is at the start - is that a good thing?
        "run --full-refresh",  # use state:modified+ if we can.
        "test",  # use state:modified+ if we can.
        "run incremental",  # use state:modified+ if we can.
        "test",  # yes again. (use state:modified+ if we can)
        "release lock on build database",
    ]

    raise NotImplementedError("dbtease test is not implemented yet.")


@cli.command()
@click.option('--project-dir', default=".")
@click.option('--profiles-dir', default="~/.dbt/")
@click.option('--schedule-dir', default=None)
@click.option('-s', '--schema', default=None)
def refresh(project_dir, profiles_dir, schedule_dir, schema):
    """Runs an appropriate refresh of the existing state."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if deployed_hash != current_hash:
        raise click.UsageError(
            f"Deployed hash is {deployed_hash}. "
            "Return to that commit to run refresh."
        )
    if status_dict["dirty_tree"]:
        raise click.UsageError(
            "Uncommitted Git changes. Please "
            "stash or discard changes to run refresh."
        )

    if schema:
        schema_options = {name for name, _ in schedule.iter_schemas()}
        if schema not in schema_options:
            raise click.UsageError(
                f"Provided schema {schema!r} not found in "
                "schedule file."
            )
        deploy_plan = [schema]
    else:
        deploy_plan = status_dict["deploy_order"]
    click.echo(f"Deploying schemas: {deploy_plan!r}")

    config_files = {
        "profiles.yml": schedule.project.generate_profiles_yml(database="foo")
    }

    with ConfigContext(file_dict=config_files) as config_path:
        print("Config Path:", config_path)

    # Always use --fail-fast
    plan = [
        "snapshot",  # currently this is at the start - is that a good thing?
        # maybe only on some schedules? I'm assuming this is not schema specific.
        "for each schema in turn..." ,
        [
            "acquire lock on build database",
            "clone live schema",  # if it's materialised, otherwise we start blank
            "run incremental",  # with defer AND with selectors for just this schema.
            "test",  # with defer and with selectors for just this schema.
            "perms",  # or do we ignore this for now?
            "acquire lock on deploy database",
            "deploy",  # schema with swap.
            "update last deploy marker for schema",  # importantly before we release lock
            "release lock on both databases",
        ]
    ]

    raise NotImplementedError("dbtease refresh is not implemented yet.")


def cli_run_command(cmd):
    retcode, stdoutlines = run_shell_command(cmd)
    if retcode != 0:
        # TODO: Better error message here.
        raise click.ClickException("Command Failed!")
    return retcode, stdoutlines


def cli_run_dbt_command(cmd):
    try:
        retcode, stdoutlines = cli_run_command(["dbt"] + cmd)
    except FileNotFoundError:
        raise click.UsageError("ERROR: dbt not found. Please install dbt.")
    return retcode, stdoutlines


@cli.command()
@click.option('--project-dir', default=".")
@click.option('--profiles-dir', default="~/.dbt/")
@click.option('--schedule-dir', default=None)
def deploy(project_dir, profiles_dir, schedule_dir):
    """Attempt to deploy the current commit as the new live version."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if status_dict["dirty_tree"] and False:  # NOTE: For testing only. REMOVE "and False" for production.
        raise click.UsageError(
            "Uncommitted Git changes. Please "
            "commit, stash or discard changes to run deploy."
        )
    if deployed_hash == current_hash:
        raise click.UsageError(
            "This commit is already deployed. To refresh, "
            "run `dbtease refresh`."
        )

    if not deployed_hash:
        click.secho(
            (
                "WARNING: No currently deployed hash, this will mean "
                "a full deploy cycle. In a large project this "
                "may take some time..."
            ),
            fg='yellow'
        )
        defer_to_state = False
    else:
        defer_to_state = True

    # Do the deploy.
    if defer_to_state:
        # Stepwise deploy
        raise NotImplementedError("dbtease partial deploy is not implemented yet.")
    else:
        # Full deploy

        # Set up our config files
        config_files = {
            "profiles.yml": schedule.project.generate_profiles_yml(database=schedule.build_config["database"])
        }
        with ConfigContext(file_dict=config_files) as config_path:
            print("Config Path:", config_path)
            profile_args = ["--profiles-dir", config_path]
            # dbt deps
            cli_run_dbt_command(["deps"])
            # make sure we've got a database to work with.

            # Try to get a lock.
            build_db_lock = schedule.warehouse.acquire_lock(schedule.build_config["database"])
            if not build_db_lock:
                raise click.UsageError(
                    "Unable to lock {0!r}. Someone else has the lock. Try again later.".format(schedule.build_config["database"])
                )
            else:
                print(f"Acquired lock: {build_db_lock}")

            # NOTE: Requires particular macros. Should make dbt package to install.
            # Or should this actually be in build into dbtease? (probably the latter).
            # This whould involve a wipe!
            cli_run_dbt_command(["run-operation", "create_build_db"] + profile_args)
            # run dbt seed
            cli_run_dbt_command(["seed"] + profile_args)
            # run dbt snapshot?
            # run dbt build --full-refresh
            # run dbt test
            # MAYBE (or maybe just test run): run dbt build (for incremental)
            # MAYBE (or maybe just test run): run dbt test
            # run dbt docs
            # upload docs
            # deploy and upload manifest in same transaction!

            # Get manifest
            with open("target/manifest.json") as manifest_file:
                manifest = manifest_file.read()
            schedule.warehouse.deploy(
                project_name=schedule.name,
                commit_hash=current_hash,
                manifest=manifest,
                build_db=schedule.build_config["database"],
                deploy_db=schedule.deploy_config["database"],
            )

            # We should check that the user currently has access to the S3 destination before building (if we're using S3 as a backend).
            # Maybe assume manifest in snowflake.
            # Otherwise this could get sticky.

        raise NotImplementedError("dbtease full deploy is not implemented yet.")

    # WE NEED TO WORK OUT HOW TO HANDLE SEEDS!

    # Always use --fail-fast
    plan = [
        "acquire lock on build database",
        "clone",  # Clone schemas that have been modified. (including seed). [unless we can't defer to state]
        "seed",  # use state:modified if we can.
        "snapshot",  # currently this is at the start - is that a good thing?
        "run --full-refresh",  # use state:modified+ if we can.
        "run incremental",  # use scheme selectors if available otherwise skip. NB: no need to full refresh tables we haven't changed if we can just do incremental on them.
        "test",  # only test once during deploy cycle - assume we tested properly the first during the PR phase.
        "docs",
        "perms",
        "acquire lock on deploy database",
        "deploy",  # as in swap database. If we only need to deploy some schemas, just do them. The others were deferred to state and so won't be complete.
        "update last deploy marker for schema",  # importantly before we release lock
        "release lock on both databases",
        "deploy",  # as in update terraform? {or is that seperate}
        "upload artifacts."  # (inluding docs)
    ]

    raise NotImplementedError("dbtease deploy is not implemented yet.")


if __name__ == '__main__':
    cli()
