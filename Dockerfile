FROM quay.io/astronomer/astro-runtime:12.9.0
RUN pip install dbt-core dbt-bigquery great_expectations==0.18.21 sqlalchemy_bigquery google-cloud-bigquery