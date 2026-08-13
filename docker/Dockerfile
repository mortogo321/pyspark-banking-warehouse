# Spark 3.5.9's bundled Hadoop client is 3.3.4 (verified by inspecting
# /opt/spark/jars/hadoop-client-api-*.jar in the base image), so the S3A
# connector and its AWS SDK dependency are pinned to the matching versions
# rather than "latest", or the two would drift apart at runtime.
FROM apache/spark:3.5.9-python3

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRESQL_JDBC_VERSION=42.7.13
ARG MAVEN_BASE=https://repo1.maven.org/maven2

# Jars are fetched at build time and baked into the image so job runs never
# depend on runtime access to Maven Central (no --packages at spark-submit time).
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      "${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
 && curl -fsSL -o /opt/spark/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
      "${MAVEN_BASE}/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar"

# pyspark itself is already provided by the base image; only the test runner
# and the seed job's S3 bucket-creation helper are added on top.
RUN pip install --no-cache-dir pytest boto3

# spark-submit sets these up for its own driver subprocess, but a bare
# `python3 -m pytest` (used by the tests service) does not, so pyspark and
# py4j are put on PYTHONPATH here too, the same way bin/pyspark does it.
ENV PYTHONPATH=/workspace/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
# The base image's entrypoint only special-cases k8s "driver"/"executor"
# commands and otherwise execs argv as-is (see /opt/entrypoint.sh), but
# /opt/spark/bin is not on PATH by default, so `spark-submit ...` alone
# would fail to resolve.
ENV PATH="/opt/spark/bin:${PATH}"
WORKDIR /workspace

USER spark
