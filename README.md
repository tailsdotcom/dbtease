# dbt runner

This is a tool for deploying a multi-part dbt project sensibly.

It handles:
- atomic swaps
- handling of full refreshes and incremental refreshes
- CI deployment
- manifest management

Config is managed using a yaml file in the dbt project.

## Architecture

- No container handling is done by the project.
- This is desinged to be run as a CLI script.
- Each schema is deployed atomically, and *can fail without affecting others*.
- Tests are run pre-deploy.
- The "clock" speed is determined externally (i.e. by whatever calls the cli).
- Internally we work out what schemas are "due" a refresh and they are refreshed in order.
- View-only schemas as not refreshed on `refresh`, but only on `deploy`.
- Stashing of metadata and logs is done by the tool to a backend (assumed S3).
- current "live" version and last refresh of each schema is handled by a database backend (sqlite in testing, astra in prod).
- `dbt seed` is only run on `deploy`.
- the warehouse target is assumed to be snowflake for now.
- liberal use of `defer` to avoid building what we don't need. Incremental models are cloned (could just clone schema to start?).
- the tool handles permissions for each schema so that hooks are not required within the dbt project.
- docs are generated on `deploy`.
- `snapshot` is assumed to run at the end of any dag, on a configurable timeschemdule.
- initially `dbt` is called as a shell function, eventually we might integrate with the python api.

## Roadmap

- initially all schema (node) details are configured directly (and not introspected). Eventually this should probably introspected.

## CLI reference

- cli should expose `deploy` and `refresh`.
  - `refresh` may run a full deploy if it deems fit.
