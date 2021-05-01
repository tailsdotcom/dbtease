# Currently testing:

dbtease refresh --schema base



> DBT WARNING PIPED DOWNSTREAM


Should make sure when doing a refresh, that we chec the commit hash once we've acquired the lock - otherwise we might be going backwards...
