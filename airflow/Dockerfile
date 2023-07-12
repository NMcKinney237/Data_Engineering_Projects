FROM quay.io/astronomer/astro-runtime:8.5.0
FROM quay.io/astronomer/astro-runtime:8.2.0


# Install apache-airflow-providers-google package
RUN pip install apache-airflow-providers-google

# Switch to root user for installing git
USER root

# Install git
RUN apt-get update && apt-get install -y git

# Clean up APT cache
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the original user
USER astro


# Create a Python virtual environment and install dbt-bigquery
ENV DBT_VENV_PATH /home/astro/dbt_venv
RUN python -m venv $DBT_VENV_PATH && \
    . $DBT_VENV_PATH/bin/activate && \
    pip install dbt-bigquery==1.5.0 