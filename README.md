# dbtease

_dbtease is a tool for sanely testing and deploying_
_large multi-schema dbt repositories in a modular way._

_dbt_ + _ease_ = _dbtease_

## Development Status

**This tool is in very early pre-alpha status,**
**we actively recommend that you DO NOT USE IT YET**.

_You have been warned_

## Ambition

dbtease handles (or _will handle_):
- atomic swaps
- handling of full refreshes and incremental refreshes
- CI deployment
- manifest management
- easy periodic refreshes on multiple schedules using a simple CLI command.

Config is managed using a yaml file in the dbt project.

## CLI reference

- `dbtease status`: Outputs the current commit and deployment status.
