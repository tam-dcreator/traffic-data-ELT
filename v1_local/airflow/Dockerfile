# Airflow runtime image for V1 local stack.
# Build context must be the repository root (set in compose.yaml via context: ..).
# Extends the official apache/airflow image with the shared traffic_data_elt
# package pre-installed.
#
# The src/ directory is also bind-mounted at runtime (compose.yaml) so
# in-place code changes are reflected without rebuilding.

ARG AIRFLOW_VERSION=3.0.3
FROM apache/airflow:${AIRFLOW_VERSION}

# Copy the shared Python package source from the repo root build context.
USER root
COPY --chown=airflow:root src/ /opt/traffic_data_elt_src/
USER airflow

# Install the shared package.
# Pin apache-airflow to the base image version to prevent pip from attempting
# an unintended upgrade or downgrade.
ARG AIRFLOW_VERSION=3.0.3
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "/opt/traffic_data_elt_src"
