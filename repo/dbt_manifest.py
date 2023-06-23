"""
Exploes a dbt DAG into an Airflow DAG by parsing the dbt manifest file

Shows the data being loaded into a postgres database as part of the DAG, normally that would be preloaded.
"""

import json
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/Users/nathanmckinney/Desktop/Github/Data_Engineering_Projects/dbt/jaffle_shop"

with DAG(
    "dbt_manifest",
    start_date=datetime(2020, 12, 23),
    description="A dbt wrapper for Airflow.",
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__
) as dag:

    def load_manifest():
        local_filepath = f"{DBT_PROJECT_DIR}/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)
        return data

    def make_dbt_task(node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""
        GLOBAL_CLI_FLAGS = "--no-write-json"
        model = node.split(".")[-1]
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=(
                    f"dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} "
                    f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
                ),
                env=DBT_ENV,
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                bash_command=(
                    f"dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} "
                    f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
                ),
                env=DBT_ENV,
            )
        return dbt_task
        

    data = load_manifest()
    dbt_tasks = {}

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            node_test = node.replace("model", "test")
            dbt_tasks[node] = make_dbt_task(node, "run")
            dbt_tasks[node_test] = make_dbt_task(node, "test")

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            # Set dependency to run tests on a model after model runs finishes
            node_test = node.replace("model", "test")
            dbt_tasks[node] >> dbt_tasks[node_test]
            # Set all model -> model dependencies
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_seed >> dbt_tasks[upstream_node] >> dbt_tasks[node]