"""Ready-made pieces for the tools pipelines already run on.

Each module imports its framework lazily, so ``import rowgate`` never pulls in
Dagster, Prefect or Airflow.

* :mod:`rowgate.integrations.dagster`: a blocking asset check for any asset
* :mod:`rowgate.integrations.prefect`: a task that publishes the report as an artifact
* :mod:`rowgate.integrations.airflow`: a callable for ``@task`` or ``PythonOperator``
* :mod:`rowgate.integrations.dbt`: turn a contract into dbt tests, and check dbt models in DuckDB
"""
