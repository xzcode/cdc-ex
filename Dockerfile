FROM node:24-bookworm-slim@sha256:873f586f3794f52281bd039bf02c048a0be08fde619338f9941bad1eb6b34530 AS web
WORKDIR /web
COPY cdc-web/package.json cdc-web/package-lock.json ./
RUN npm ci
COPY cdc-web/ ./
RUN npm run build

FROM maven:3.9.11-eclipse-temurin-21@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237 AS build
WORKDIR /src
COPY . .
COPY --from=web /web/dist ./cdc-web/dist
RUN mvn -B -ntp -DskipTests package

FROM eclipse-temurin:21-jre-jammy@sha256:a8bff88f760e87e0c8844f8461f807c203fd5e8f78c951c843024647e965eaf5 AS runtime
RUN groupadd --gid 10001 cdc && useradd --uid 10001 --gid cdc --no-create-home cdc \
    && mkdir -p /app /data && chown cdc:cdc /app /data
COPY --chmod=755 cdc-distribution/healthcheck.sh /app/healthcheck.sh
ENV CDC_MODE=server CDC_DATA_DIR=/data CDC_BIND=127.0.0.1 CDC_PORT=8080
USER 10001:10001
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD ["/app/healthcheck.sh"]
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-Duser.timezone=UTC","-jar","/app/cdc-ex.jar"]

# CI supplies a separate, checksum-verified named context; no second Maven build.
FROM runtime AS tested
COPY --from=tested-artifact cdc-ex.jar /app/cdc-ex.jar

# Default target: keep the normal source-build experience for Docker Compose.
FROM runtime AS source
COPY --from=build /src/cdc-server/target/cdc-server-*.jar /app/cdc-ex.jar
