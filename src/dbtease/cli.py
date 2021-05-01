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
    click.secho(
        f"Running: {' '.join(cmd)}",
        fg='blue'
    )
    retcode, stdoutlines,stderrlines = run_shell_command(cmd, echo=click.echo)
    if retcode != 0:
        # TODO: Better error message here.
        for errline in stderrlines:
            click.echo(errline)
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
@click.option('-f', '--force', is_flag=True, help="Force a full deploy cycle.")
def deploy(project_dir, profiles_dir, schedule_dir, force):
    """Attempt to deploy the current commit as the new live version."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if status_dict["dirty_tree"]:
        raise click.UsageError(
            "Uncommitted Git changes. Please "
            "commit, stash or discard changes to run deploy."
        )
    if deployed_hash == current_hash and not force:
        raise click.UsageError(
            "This commit is already deployed. To refresh, "
            "run `dbtease refresh`."
        )

    if not deployed_hash or force:
        if force:
            full_reason = "Forcing a full deploy."
        else:
            full_reason = "No current deployment. This forces a full deploy."
        click.secho(
            f"WARNING: {full_reason} In a large project this may take some time...",
            fg='yellow'
        )
        defer_to_state = False
    else:
        click.secho(
            "ATTEMPTING PARTIAL DEPLOY",
            fg='cyan'
        )
        defer_to_state = True

    # Do the deploy.
    # Set up our config files
    with ConfigContext(file_dict={
        # Use build context first
        "profiles.yml": schedule.project.generate_profiles_yml(database=schedule.build_config["database"])
    }) as ctx:
        profile_args = ["--profiles-dir", str(ctx)]
        # if we're going to upload docs, check we have access.
        if schedule.filestore:
            if not schedule.filestore.check_access():
                raise click.ClickException("Access check to filestore failed. Make sure you have access.")
        # dbt deps
        cli_run_dbt_command(["deps"])
        # Deploy
        # Try to get a lock on the build database
        click.secho("Acquiring Build Lock", fg='blue')
        with schedule.warehouse.lock(schedule.build_config["database"]):
            if defer_to_state:
                # NOTE: Although we only need to update the changed models, we still have to
                # deploy monolithically do make sure dependencies don't break.
                # We clone the existing deployment. No need to rely on state, because we'll rebuild
                # whole schemas, but we do need the downstream schemas to exist.
                # NOTE: This means we shouldn't update the "last_deployed" timestamp on all of the
                # schemas, only the ones we rebuilt.

                # make sure we've got a database to work with.
                click.secho("Cloning live database", fg='blue')
                schedule.warehouse.create_wipe_db(
                    schedule.build_config["database"],
                    source=schedule.deploy_config["database"]
                )

                # Build each schema individually, but deploy in one transaction.
                for schema_name in status_dict["deploy_order"]:
                    click.secho(f"BUILDING: {schema_name}", fg='cyan')
                    schema = schedule.get_schema(schema_name)
                    # run dbt seed
                    cli_run_dbt_command(["seed", "--select", schema.selector()] + profile_args)
                    # run dbt build --full-refresh
                    cli_run_dbt_command(["run", "--models", schema.selector(), "--full-refresh", "--fail-fast"] + profile_args)
                    # run dbt test
                    cli_run_dbt_command(["test", "--models", schema.selector()] + profile_args)
            else:     
                # make sure we've got a database to work with.
                click.secho("Initialising build database", fg='blue')
                schedule.warehouse.create_wipe_db(schedule.build_config["database"])
                # run dbt seed
                cli_run_dbt_command(["seed"] + profile_args)
                # run dbt snapshot?
                # run dbt build --full-refresh
                cli_run_dbt_command(["run", "--full-refresh", "--fail-fast"] + profile_args)
                # run dbt test
                cli_run_dbt_command(["test"] + profile_args)
                # MAYBE (or maybe just test run): run dbt build (for incremental)
                # MAYBE (or maybe just test run): run dbt test
                
            # Get lock on deploy DB
            click.secho("Acquiring Deploy Lock", fg='blue')
            with schedule.warehouse.lock(schedule.deploy_config["database"]):
                # Deploy
                click.secho("Deploying...", fg='blue')
                schedule.warehouse.deploy(
                    project_name=schedule.name,
                    commit_hash=current_hash,
                    # NB, no manifest on deploy. A NULL Manifest means other clients should wait briefly for it!
                    build_db=schedule.build_config["database"],
                    deploy_db=schedule.deploy_config["database"],
                )

        # Update to deploy context to build and update docs.
        click.secho("Updating to deploy context", fg='blue')
        with ctx.patch_files({
            "profiles.yml": schedule.project.generate_profiles_yml(database=schedule.deploy_config["database"])
        }):
            # dbt docs (which also generates manifest). NB: We're using the DEPLOY context so the references work.
            # For the same reason we still need profile args.
            cli_run_dbt_command(["docs", "generate"] + profile_args)
            # Stash the docs and the manifest
            ctx.stash_files("target/manifest.json", "target/catalog.json", "target/index.html")
            # Get manifest
            manifest = ctx.read_file("manifest.json")
            # Build docs and update manifest.
            click.secho("Updating Manifest.", fg='blue')
            schedule.warehouse.deploy_manifest(
                project_name=schedule.name,
                commit_hash=current_hash,
                manifest=manifest,
            )
        # Upload docs here.
        if schedule.filestore:
            schedule.filestore.upload_files("target/manifest.json", "target/catalog.json", "target/index.html")
    click.secho("DONE", fg='green')

if __name__ == '__main__':
    cli()
