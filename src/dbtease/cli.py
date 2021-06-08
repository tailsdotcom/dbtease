"""CLI methods."""

import click
import logging
import sys
import datetime

from dbtease.schedule import DbtSchedule

from dbtease.config_context import ConfigContext
from dbtease.shell import run_shell_command

from dbtease.dbt import diff_manifests


# Set up logging properly
root = logging.getLogger("dbtease")
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
root.addHandler(ch)


@click.group()
@click.version_option()
def cli():
    pass


def common_setup(
    project_dir, profiles_dir, schedule_dir, deploy=True, aws_profile=None
):
    schedule_dir = schedule_dir or project_dir
    # Load the schedule
    schedule = DbtSchedule.from_path(
        schedule_dir,
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        aws_profile=aws_profile,
    )
    status_dict = schedule.status_dict(deploy=deploy)
    return schedule, status_dict


def echo_status(status_dict, project_name):
    click.echo("=== dbtease status ===")
    config_pairs = [
        ("Deployment Name", project_name),
        ("Deployed Hash", status_dict["deployed_hash"]),
        ("Current Hash", status_dict["current_hash"]),
        ("Uncommitted Changes", status_dict["dirty_tree"]),
        ("Redeploy Due", status_dict["redeploy_due"]),
        ("Refreshes Due", ", ".join(status_dict["refreshes_due"])),
    ]
    for label, value in config_pairs:
        click.echo(f"{label:22} - {value}")
    click.echo("===")


def echo_plan(plan_dict):
    click.echo("=== deploy plan ===")
    config_pairs = [
        ("Deployment Plan", ", ".join(plan_dict["deploy_order"])),
        ("Triggers Full Deploy", plan_dict["trigger_full_deploy"]),
    ]
    for label, value in config_pairs:
        click.echo(f"{label:22} - {value}")
    # Output Files affected
    if plan_dict["unmatched_files"]:
        click.secho("== unmatched files ==", fg="red")
        for fname in plan_dict["unmatched_files"]:
            click.echo(f"- {fname}")
    for schema_name in plan_dict["matched_files"]:
        click.secho(f"== schema: {schema_name} ==", fg="yellow")
        for fname in plan_dict["matched_files"][schema_name]:
            click.echo(f"- {fname}")
    click.echo("===")


def get_compiled_manifest(schedule):
    with ConfigContext(
        file_dict={
            # Use deploy context
            "profiles.yml": schedule.project.generate_profiles_yml(
                database=schedule.deploy_config["database"],
                schema=schedule.schema_prefix,
            )
        }
    ) as ctx:
        profile_args = ["--profiles-dir", str(ctx)]
        # dbt deps
        cli_run_dbt_command(["deps"])
        # Compile to generate manifest
        cli_run_dbt_command(["compile"] + profile_args)
        # Stash the docs and the manifest
        ctx.stash_files("target/manifest.json")
        # Get manifest
        new_manifest = ctx.read_file("manifest.json")
        return new_manifest


def generate_plan(schedule, status_dict):
    """Generate a plan from a manifest diff."""
    # Fetch manifest of current live build
    live_manifest = schedule.warehouse.fetch_manifest(
        schedule.name, status_dict["deployed_hash"]
    )
    # Compiled Manifest
    new_manifest = get_compiled_manifest(schedule)
    node_diff = diff_manifests(live_manifest, new_manifest)

    paths = [path for _, path in node_diff]
    plan = schedule.generate_plan_from_paths(paths)
    if not node_diff:
        click.secho("NO MODELS CHANGED", fg="green")
    else:
        echo_plan(plan)
    return plan, new_manifest


@cli.command()
@click.option("--project-dir", default=".")
@click.option("--profiles-dir", default="~/.dbt/")
@click.option("--schedule-dir", default=None)
def status(project_dir, profiles_dir, schedule_dir):
    """Get the current status of deployment."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Output the status.
    echo_status(status_dict, schedule.name)
    if (
        status_dict["current_hash"] == status_dict["deployed_hash"]
        and not status_dict["dirty_tree"]
    ):
        click.secho("ON CURRENT LIVE COMMIT", fg="green")
        return

    click.secho("Hash differs or tree is dirty. Generating a manifest...\n", fg="cyan")
    generate_plan(schedule, status_dict)


@cli.command()
@click.option("--project-dir", default=".")
@click.option("--profiles-dir", default="~/.dbt/")
@click.option("--schedule-dir", default=None)
@click.option("--database", default=None)
@click.option("--append-commit-to-db", is_flag=True)
def test(project_dir, profiles_dir, schedule_dir, database, append_commit_to_db):
    """Tests the current active changes."""
    schedule, status_dict = common_setup(project_dir, profiles_dir, schedule_dir)
    # Output the status.
    echo_status(status_dict, schedule.name)
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if deployed_hash == current_hash and not status_dict["dirty_tree"]:
        click.secho("No changes made compared to deployed version.", fg="yellow")
        return

    build_db = database or schedule.project.get_default_database()
    if append_commit_to_db:
        # Use the first eight characters of the hash only because otherwise it gets really long.
        build_db += "_" + current_hash[:8]

    file_dict = {
        "profiles.yml": schedule.project.generate_profiles_yml(
            database=build_db, schema=schedule.schema_prefix
        )
    }

    if not deployed_hash:
        click.secho(
            (
                "WARNING: No currently deployed hash, this will mean "
                "a full test build. In a large project this "
                "may take some time..."
            ),
            fg="yellow",
        )
        defer_to_state = False
    else:
        defer_to_state = True
        # Fetch manifest of current live build
        file_dict["manifest.json"] = schedule.warehouse.fetch_manifest(
            schedule.name, deployed_hash
        )

    try:
        # Set up our config files
        with ConfigContext(file_dict=file_dict) as ctx:
            profile_args = ["--profiles-dir", str(ctx)]
            # dbt deps
            cli_run_dbt_command(["deps"])
            # Deploy
            # Try to get a lock on the build database
            click.secho("Acquiring Build Lock", fg="bright_blue")
            with schedule.warehouse.lock(build_db):
                # make sure we've got a database to work with.
                click.secho("Cleaning test database", fg="bright_blue")
                schedule.warehouse.create_wipe_db(build_db)
                if defer_to_state:
                    # run dbt seed
                    cli_run_dbt_command(
                        [
                            "seed",
                            "--select",
                            "state:modified",
                            "--full-refresh",
                            "--state",
                            str(ctx),
                        ]
                        + profile_args
                    )
                    # run dbt. NOTE: full refresh + to also do donwstream dependencies. Defer so we don't build what we don't need.
                    cli_run_dbt_command(
                        [
                            "run",
                            "--models",
                            "state:modified+",
                            "--full-refresh",
                            "--fail-fast",
                            "--defer",
                            "--state",
                            str(ctx),
                        ]
                        + profile_args
                    )
                    # dbt test. with dependencies
                    cli_run_dbt_command(
                        [
                            "test",
                            "--models",
                            "state:modified+",
                            "--defer",
                            "--state",
                            str(ctx),
                        ]
                        + profile_args
                    )
                    # run - incrementally this time (but only run the models which are incremental and their dependencies)
                    cli_run_dbt_command(
                        [
                            "run",
                            "--models",
                            "state:modified+,config.materialized:incremental+",
                            "--defer",
                            "--fail-fast",
                            "--state",
                            str(ctx),
                        ]
                        + profile_args
                    )
                    # dbt test again, with dependencies
                    cli_run_dbt_command(
                        [
                            "test",
                            "--models",
                            "state:modified+,config.materialized:incremental+",
                            "--defer",
                            "--state",
                            str(ctx),
                        ]
                        + profile_args
                    )
                else:
                    # run dbt seed
                    cli_run_dbt_command(["seed", "--full-refresh"] + profile_args)
                    # run dbt build --full-refresh
                    cli_run_dbt_command(
                        ["run", "--full-refresh", "--fail-fast"] + profile_args
                    )
                    # run dbt test
                    cli_run_dbt_command(["test"] + profile_args)
                    # run incrementally
                    cli_run_dbt_command(
                        [
                            "run",
                            "--models",
                            "config.materialized:incremental+",
                            "--fail-fast",
                        ]
                        + profile_args
                    )
                    # dbt test again, with dependencies
                    cli_run_dbt_command(
                        ["test", "--models", "config.materialized:incremental+"]
                        + profile_args
                    )
        click.secho("SUCCESS", fg="green")
        schedule.handle_event(
            "test_success",
            success=True,
            message="Successful Test",
            metadata={"hash": current_hash},
        )
    except Exception as err:
        click.secho("FAIL", fg="red")
        schedule.handle_event(
            "test_fail",
            success=False,
            message="Failed Test",
            metadata={"hash": current_hash},
        )
        raise err


def cli_run_command(cmd):
    click.secho(f"Running: {' '.join(cmd)}", fg="bright_blue")
    retcode, stdoutlines, stderrlines = run_shell_command(cmd, echo=click.echo)
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


def schemawise_refresh(deploy_plan, schedule, manifest, current_hash):
    # dbt deps
    cli_run_dbt_command(["deps"])
    # Iterate Schemas to Deploy
    for schema_name in deploy_plan:
        click.secho(f"BUILDING: {schema_name}", fg="cyan")
        schema = schedule.get_schema(schema_name)
        build_db = (
            schema.build_config.get("database", None)
            or schedule.build_config["database"]
        )
        # Set up context
        with ConfigContext(
            file_dict={
                "profiles.yml": schedule.project.generate_profiles_yml(
                    database=build_db,
                    schema=schedule.schema_prefix,
                ),
                "manifest.json": manifest,
            }
        ) as ctx:
            profile_args = ["--profiles-dir", str(ctx)]
            # defer only works for run and test
            defer_args = ["--defer", "--state", str(ctx)]
            # Acquire lock on build database
            with schedule.warehouse.lock(build_db):
                build_timestamp = datetime.datetime.utcnow()
                # Make a blank build database.
                click.secho(
                    f"Creating clean build database: {build_db!r}", fg="bright_blue"
                )
                schedule.warehouse.create_wipe_db(build_db)
                # If it's a materialised schema, clone the live version into it
                if schema.materialized:
                    for idx, sch in enumerate(schema.schemas):
                        click.secho(
                            f"Cloning live schema: {sch!r} [{idx + 1}/{len(schema.schemas)}]",
                            fg="bright_blue",
                        )
                        schedule.warehouse.clone_schema(
                            sch, build_db, source=schedule.deploy_config["database"]
                        )
                # Refresh the schema (NB: Incremental)
                # NOTE: No seeds, because they're assumed unchanged.
                cli_run_dbt_command(
                    ["run", "--models", schema.selector(), "--fail-fast"]
                    + profile_args
                    + defer_args
                )
                # run dbt test
                cli_run_dbt_command(
                    ["test", "--models", schema.selector(), "--fail-fast"]
                    + profile_args
                    + defer_args
                )
                # Deploy schema
                # Get lock on deploy DB
                click.secho("Acquiring Deploy Lock", fg="bright_blue")
                with schedule.warehouse.lock(schedule.deploy_config["database"]):
                    # Deploy
                    click.secho("Deploying...", fg="bright_blue")
                    schedule.warehouse.deploy_schemas(
                        project_name=schedule.name,
                        commit_hash=current_hash,
                        schemas=schema.schemas,
                        build_db=build_db,
                        deploy_db=schedule.deploy_config["database"],
                        build_timestamp=build_timestamp,
                    )
                    schedule.handle_event(
                        "refresh_success",
                        success=True,
                        message="Successful Refresh",
                        metadata={"schema_name": schema_name, "hash": current_hash},
                    )


def database_deploy(schedule, current_hash, defer_to_state, deploy_order):
    # Do the deploy.
    build_timestamp = datetime.datetime.utcnow()
    # Set up our config files
    with ConfigContext(
        file_dict={
            # Use build context first
            "profiles.yml": schedule.project.generate_profiles_yml(
                database=schedule.build_config["database"],
                schema=schedule.schema_prefix,
            )
        }
    ) as ctx:
        profile_args = ["--profiles-dir", str(ctx)]
        # if we're going to upload docs, check we have access.
        if schedule.filestore:
            if not schedule.filestore.check_access():
                raise click.ClickException(
                    "Access check to filestore failed. Make sure you have access."
                )
        # dbt deps
        cli_run_dbt_command(["deps"])
        # Deploy
        # Try to get a lock on the build database
        click.secho("Acquiring Build Lock", fg="bright_blue")
        with schedule.warehouse.lock(schedule.build_config["database"]):
            if defer_to_state:
                # NOTE: Although we only need to update the changed models, we still have to
                # deploy monolithically do make sure dependencies don't break.
                # We clone the existing deployment. No need to rely on state, because we'll rebuild
                # whole schemas, but we do need the downstream schemas to exist.
                # NOTE: This means we shouldn't update the "last_deployed" timestamp on all of the
                # schemas, only the ones we rebuilt.

                # make sure we've got a database to work with.
                click.secho("Cloning live database", fg="bright_blue")
                schedule.warehouse.create_wipe_db(
                    schedule.build_config["database"],
                    source=schedule.deploy_config["database"],
                )

                # Build each schema individually, but deploy in one transaction.
                for idx, schema_name in enumerate(deploy_order):
                    click.secho(
                        f"BUILDING: {schema_name} [{idx + 1}/{len(deploy_order)}]",
                        fg="cyan",
                    )
                    schema = schedule.get_schema(schema_name)
                    # run dbt seed
                    cli_run_dbt_command(
                        ["seed", "--select", schema.selector(), "--full-refresh"]
                        + profile_args
                    )
                    # run dbt build --full-refresh
                    cli_run_dbt_command(
                        [
                            "run",
                            "--models",
                            schema.selector(),
                            "--full-refresh",
                            "--fail-fast",
                        ]
                        + profile_args
                    )
                    # run dbt test
                    cli_run_dbt_command(
                        ["test", "--models", schema.selector()] + profile_args
                    )
            else:
                # make sure we've got a database to work with.
                click.secho("Initialising build database", fg="bright_blue")
                schedule.warehouse.create_wipe_db(schedule.build_config["database"])
                # run dbt seed
                cli_run_dbt_command(["seed", "--full-refresh"] + profile_args)
                # run dbt snapshot?
                # run dbt build --full-refresh
                cli_run_dbt_command(
                    ["run", "--full-refresh", "--fail-fast"] + profile_args
                )
                # run dbt test
                cli_run_dbt_command(["test"] + profile_args)

            # Get lock on deploy DB
            click.secho("Acquiring Deploy Lock", fg="bright_blue")
            with schedule.warehouse.lock(schedule.deploy_config["database"]):
                # Deploy
                click.secho("Deploying...", fg="bright_blue")
                schedule.warehouse.deploy(
                    project_name=schedule.name,
                    commit_hash=current_hash,
                    schemas=[schema_name for schema_name, _ in schedule.iter_schemas()],
                    # NB, no manifest on deploy. A NULL Manifest means other clients should wait briefly for it!
                    build_db=schedule.build_config["database"],
                    deploy_db=schedule.deploy_config["database"],
                    build_timestamp=build_timestamp,
                )
                schedule.handle_event(
                    "deploy_success",
                    success=True,
                    message="Successful Deploy",
                    metadata={"hash": current_hash},
                )

        # Update to deploy context to build and update docs.
        click.secho("Updating to deploy context", fg="bright_blue")
        with ctx.patch_files(
            {
                "profiles.yml": schedule.project.generate_profiles_yml(
                    database=schedule.deploy_config["database"],
                    schema=schedule.schema_prefix,
                )
            }
        ):
            # dbt docs (which also generates manifest). NB: We're using the DEPLOY context so the references work.
            # For the same reason we still need profile args.
            cli_run_dbt_command(["docs", "generate"] + profile_args)
            # Stash the docs and the manifest
            ctx.stash_files(
                "target/manifest.json", "target/catalog.json", "target/index.html"
            )
            # Get manifest
            manifest = ctx.read_file("manifest.json")
            # Build docs and update manifest.
            click.secho("Updating Manifest.", fg="bright_blue")
            schedule.warehouse.deploy_manifest(
                project_name=schedule.name,
                commit_hash=current_hash,
                manifest=manifest,
            )
        # Upload docs here.
        if schedule.filestore:
            click.secho("Uploading Docs.", fg="bright_blue")
            schedule.filestore.upload_files(
                "target/manifest.json", "target/catalog.json", "target/index.html"
            )
            schedule.handle_event(
                "upload_docs_success",
                success=True,
                message="Successful Docs Upload",
                metadata={"hash": current_hash},
            )


@cli.command()
@click.option("--project-dir", default=".")
@click.option("--profiles-dir", default="~/.dbt/")
@click.option("--schedule-dir", default=None)
@click.option("-s", "--schema", default=None)
def refresh(project_dir, profiles_dir, schedule_dir, schema):
    """Runs an appropriate refresh of the existing state."""
    schedule, status_dict = common_setup(
        project_dir, profiles_dir, schedule_dir, deploy=False
    )
    # Output the status.
    echo_status(status_dict, schedule.name)
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
                f"Provided schema {schema!r} not found in " "schedule file."
            )
        deploy_plan = [schema]
    else:
        deploy_plan = status_dict["refreshes_due"]

    if not deploy_plan:
        click.secho("No refreshes due...", fg="green")
    else:
        # Fetch manifest of current live build
        manifest = schedule.warehouse.fetch_manifest(schedule.name, deployed_hash)
        # If redeploy is due, then do a redeploy.
        if status_dict["redeploy_due"]:
            click.secho(
                "WARNING: Full redeploy is due. This may take some time on a large project.",
                fg="yellow",
            )
            database_deploy(
                schedule,
                current_hash,
                defer_to_state=False,
                deploy_order=status_dict["deploy_order"],
            )
        else:
            click.secho(f"Refreshing schemas: {deploy_plan!r}", fg="cyan")
            # Refresh cycle.
            schemawise_refresh(deploy_plan, schedule, manifest, current_hash)
    click.secho("DONE", fg="green")


@cli.command()
@click.option("--project-dir", default=".")
@click.option("--profiles-dir", default="~/.dbt/")
@click.option("--schedule-dir", default=None)
@click.option("--aws-profile", default=None)
@click.option("-f", "--force", is_flag=True, help="Force a full deploy cycle.")
def deploy(project_dir, profiles_dir, schedule_dir, aws_profile, force):
    """Attempt to deploy the current commit as the new live version."""
    schedule, status_dict = common_setup(
        project_dir, profiles_dir, schedule_dir, aws_profile=aws_profile
    )
    # Output the status.
    echo_status(status_dict, schedule.name)
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
            "This commit is already deployed. To refresh, " "run `dbtease refresh`."
        )

    # Test permissions
    if schedule.filestore:
        if not schedule.filestore.check_access():
            raise click.UsageError(
                "Test upload failed. Confirm you have appropriate permissions to upload to filestore."
            )

    deploy_order = []
    trigger_full_deploy = False

    if deployed_hash and not force:
        click.secho(
            "\nGenerating Manifest to plan deploy...",
            fg="cyan",
        )
        plan, manifest = generate_plan(schedule, status_dict)
        deploy_order = plan["deploy_order"]
        trigger_full_deploy = plan["trigger_full_deploy"]

        if not deploy_order:
            click.secho("Manifest indicates no model changes....", fg="green")
            # Build docs and update manifest.
            click.secho("\nUpdating Manifest.", fg="bright_blue")
            schedule.warehouse.deploy_manifest(
                project_name=schedule.name,
                commit_hash=current_hash,
                manifest=manifest,
                update_commit=True,
            )
            schedule.handle_event(
                "deploy_success",
                success=True,
                message="Successful Non-Project Deploy",
                metadata={"hash": current_hash},
            )
            click.secho("\nDONE", fg="green")
            return

    if not deployed_hash or force or trigger_full_deploy:
        if force:
            full_reason = "Forcing a full deploy."
        elif trigger_full_deploy:
            full_reason = "Full deploy triggered by a changed schema."
        else:
            full_reason = "No current deployment. This forces a full deploy."
        click.secho(
            f"WARNING: {full_reason} In a large project this may take some time...",
            fg="yellow",
        )
        defer_to_state = False
    else:
        click.secho(
            f"ATTEMPTING PARTIAL DEPLOY: {', '.join(deploy_order)}",
            fg="cyan",
        )
        defer_to_state = True

    # Do the deploy.
    database_deploy(schedule, current_hash, defer_to_state, deploy_order)
    click.secho("DONE", fg="green")


if __name__ == "__main__":
    cli()
