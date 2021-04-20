import click
import os.path
import yaml

from collections import defaultdict

from dbtease.git import get_git_state
from dbtease.repository import JsonStateRepository
from dbtease.schedule import DbtSchedule


@click.group()
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
    # Load state
    deploy_state_repo = JsonStateRepository()
    deployed_hash = deploy_state_repo.get_current_deployed()
    click.echo(repr(deployed_hash))
    # Introspect git status
    git_status = get_git_state(deployed_hash=deployed_hash)
    click.echo(repr(git_status))
    # Load the schedule
    schedule = DbtSchedule.from_path(".")
    click.echo(schedule.graph.nodes)
    changed_files = git_status["diff"] | git_status["untracked"]
    matched_files = set()
    for schema, files in schedule.iter_affected_schemas(paths=changed_files):
        click.echo(repr(schema))
        matched_files |= files
        click.echo(repr(files))
        # Should work out how to deal with the remainder explicitly.
    click.echo("Unmatched files: {}".format(repr(changed_files - matched_files)))

if __name__ == '__main__':
    cli()
