import click
import os.path
import yaml

from collections import defaultdict

from dbtease.git import get_git_state
from dbtease.repository import JsonStateRepository
from dbtease.schedule import DbtSchedule


@click.group()
@click.version_option()
def cli():
    pass


def iter_schemas(models, path=None):
    """Iterate schema tuples from a models object.
    yields: schema, path, materialised
    """
    path = path or []
    for key, val in models.items():
        if isinstance(val, dict):
            new_path = path + [key]
            schema = None
            for sub_key in val:
                if sub_key.endswith("schema"):
                    schema = val[sub_key]
            materialized = None
            for sub_key in val:
                if sub_key.endswith("materialized"):
                    materialized = val[sub_key]
            if schema:
                if not materialized:
                    print(
                        "Default materialisation not set for path {0}.".format(
                            new_path
                        )
                    )
                else:
                    yield (val[sub_key], new_path, materialized)
            yield from iter_schemas(val, path=new_path)


def combine_schemas(models):
    schema_dict = defaultdict(list)
    for schema, path, materialised in iter_schemas(models):
        schema_dict[schema].append((path, materialised))
    return schema_dict


@cli.command()
@click.option('--dbt-dir', prompt='Location of your dbt_project.yml',
              help='Location of your dbt_project.yml')
def check(dbt_dir):
    click.echo('Checking yaml config...')
    with open(os.path.join(dbt_dir, "dbt_project.yml")) as dbt_proj_file:
        dbt_project = yaml.safe_load(dbt_proj_file.read())
    click.echo(dbt_project['models'])
    schemas = list(iter_schemas(dbt_project['models']))
    click.echo(schemas)
    click.echo(combine_schemas(dbt_project['models']))


@cli.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)


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

    # If we're in test, surely we're just testing the modified ones (using state:modified)?
    # (but we do a test full and a test incremental)

    # Deploy schemas are any materialised schemas and any schemas changed.
    # For each of these, we clone (optionally? if materialised), run, test, deploy.


if __name__ == '__main__':
    cli()
