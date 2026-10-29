# Spark 4.2.0's bundled Hadoop client is 3.5.0 (verified via /opt/spark/RELEASE
# and /opt/spark/jars/hadoop-client-api-*.jar in the base image), so the S3A
# connector is pinned to the same version. Hadoop 3.4+ S3A is built on AWS SDK
# v2: its dependencies are software.amazon.awssdk:bundle (not the v1
# aws-java-sdk-bundle) and analyticsaccelerator-s3, at the versions declared in
# hadoop-project-3.5.0.pom (aws-java-sdk-v2.version /
# amazon-s3-analyticsaccelerator-s3.version). Bump all four together.
FROM apache/spark:4.2.0-python3

USER root

ARG HADOOP_AWS_VERSION=3.5.0
ARG AWS_SDK_V2_BUNDLE_VERSION=2.35.4
ARG S3_ANALYTICS_ACCELERATOR_VERSION=1.3.1
ARG POSTGRESQL_JDBC_VERSION=42.7.14
ARG MAVEN_BASE=https://repo1.maven.org/maven2

# Jars are fetched at build time and baked into the image so job runs never
# depend on runtime access to Maven Central (no --packages at spark-submit time).
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSL -o /opt/spark/jars/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar \
      "${MAVEN_BASE}/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar" \
 && curl -fsSL -o /opt/spark/jars/analyticsaccelerator-s3-${S3_ANALYTICS_ACCELERATOR_VERSION}.jar \
      "${MAVEN_BASE}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${S3_ANALYTICS_ACCELERATOR_VERSION}.jar" \
 && curl -fsSL -o /opt/spark/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
      "${MAVEN_BASE}/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar"

# pyspark itself is already provided by the base image; only the test runner
# and the seed job's S3 bucket-creation helper are added on top. Ranges match
# pyproject.toml so image rebuilds stay up-to-date via Dependabot (pip).
RUN pip install --no-cache-dir "pytest>=8,<10" "boto3>=1.34,<2"

# spark-submit sets these up for its own driver subprocess, but a bare
# `python3 -m pytest` (used by the tests service) does not, so pyspark and
# py4j are put on PYTHONPATH here too, the same way bin/pyspark does it.
ENV PYTHONPATH=/workspace/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
# The base image's entrypoint only special-cases k8s "driver"/"executor"
# commands and otherwise execs argv as-is (see /opt/entrypoint.sh), but
# /opt/spark/bin is not on PATH by default, so `spark-submit ...` alone
# would fail to resolve.
ENV PATH="/opt/spark/bin:${PATH}"
WORKDIR /workspace

USER spark
