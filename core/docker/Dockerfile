# syntax=docker/dockerfile:1-labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ghcr.io/airlift/jvmkill:latest AS jvmkill

FROM redhat/ubi10-minimal:latest AS jdk-download
ARG JDK_DOWNLOAD_LINK
ARG JDK_VERSION
ENV JAVA_HOME="/usr/lib/jvm/${JDK_VERSION}"

RUN \
    set -xeuo pipefail && \
    microdnf install -y tar gzip && \
    # Install JDK from the provided archive link \
    echo "Downloading JDK from ${JDK_DOWNLOAD_LINK}" && \
    mkdir -p "${JAVA_HOME}" && \
    curl -#LfS "${JDK_DOWNLOAD_LINK}" | tar -zx --strip 1 -C "${JAVA_HOME}"

FROM redhat/ubi10 AS packages

RUN \
    set -xeuo pipefail && \
    mkdir -p /tmp/overlay/usr/libexec/ && \
    touch /tmp/overlay/usr/libexec/grepconf.sh && \
    chmod +x /tmp/overlay/usr/libexec/grepconf.sh && \
    yum update -y && \
    yum install --installroot /tmp/overlay --setopt install_weak_deps=false --nodocs -y \
      less \
      libstdc++ `# required by snappy and duckdb` \
      curl-minimal grep `# required by health-check` \
      zlib `#required by java` \
      shadow-utils `# required by useradd` \
      tar `# required to support kubectl cp` && \
      rm -rf /tmp/overlay/var/cache/*

# Use ubi10 micro as it's more secure
FROM redhat/ubi10-micro:latest
ARG JDK_VERSION
ARG ARCH
ENV JAVA_HOME="/usr/lib/jvm/${JDK_VERSION}"
ENV PATH=$PATH:$JAVA_HOME/bin
ENV CATALOG_MANAGEMENT=static
COPY --from=jdk-download $JAVA_HOME $JAVA_HOME
COPY --from=packages /tmp/overlay /

RUN \
    set -xeu && \
    groupadd trino --gid 1000 && \
    useradd trino --uid 1000 --gid 1000 --create-home && \
    mkdir -p /usr/lib/trino /data/trino && \
    chown -R "trino:trino" /usr/lib/trino /data/trino

COPY --chown=trino:trino trino-cli.jar /usr/bin/trino
COPY --chown=trino:trino --exclude=bin/darwin-* --exclude=bin/linux-* trino-server /usr/lib/trino
COPY --chown=trino:trino trino-server/bin/linux-${ARCH} /usr/lib/trino/bin/linux-${ARCH}
COPY --chown=trino:trino default/etc /etc/trino
COPY --chown=trino:trino --from=jvmkill /libjvmkill.so /usr/lib/trino/bin

EXPOSE 8080
USER trino:trino
CMD ["/usr/lib/trino/bin/run-trino"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /usr/lib/trino/bin/health-check
