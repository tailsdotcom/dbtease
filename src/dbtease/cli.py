import click
import os.path
import yaml

from collections import defaultdict

from dbtease.schedule import DbtSchedule


@click.group()
@click.version_option()
def cli():
    pass


@cli.command()
def status():
    """Get the current status of deployment."""
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    status_dict = schedule.status_dict()
    # Output the status.
    click.echo("=== dbtease status ===")
    config_pairs = [
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
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    status_dict = schedule.status_dict()
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if deployed_hash == current_hash and not status_dict["dirty_tree"]:
        raise click.UsageError(
            "No changes made compared to deployed version. "
            "To build regardless, call `dbt run` directly."
        )
    raise NotImplementedError("dbtease test is not implemented yet.")


@cli.command()
def refresh():
    """Runs an appropriate refresh of the existing state."""
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    status_dict = schedule.status_dict()
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
    raise NotImplementedError("dbtease refresh is not implemented yet.")


@cli.command()
@click.option(
    "--force-backend-update",
    is_flag=True,
    help=(
        "Skip any test or build and force the state backend to "
        "recognise the current build as live. BE VERY CAREFUL "
        "WITH THIS IN PRODUCTION."
    )
)
def deploy(force_backend_update):
    """Attempt to deploy the current commit as the new live version."""
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    status_dict = schedule.status_dict()
    # Validate state
    deployed_hash = status_dict["deployed_hash"]
    current_hash = status_dict["current_hash"]
    if status_dict["dirty_tree"]:
        raise click.UsageError(
            "Uncommitted Git changes. Please "
            "commit, stash or discard changes to run deploy."
        )
    if deployed_hash == current_hash:
        raise click.UsageError(
            "This commit is already deployed. To refresh, "
            "run `dbtease refresh`."
        )
    
    # Check for force mode
    if force_backend_update:
        click.echo(f"Setting current deployed hash to {current_hash}...")
        schedule.state_repository.set_current_deployed(
            commit_hash=current_hash
        )
        click.secho('SUCCESS', fg='green')
        return

    raise NotImplementedError("dbtease deploy is not implemented yet.")


if __name__ == '__main__':
    cli()
