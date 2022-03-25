# Use a volume to store the maven downloads?? What will happen if multiple builds happen on the same volume ??
# Snapshot dependencies will most likely cause issues maybe seperate them into another folder ??
FROM maven:3.8.2-openjdk-8-slim as base

WORKDIR /app

COPY pom.xml .
# Resolve dependencies only by using pom.xml to ensure dependencies are not downloaded again if pom.xml remains same even if the code changes
RUN mvn -B dependency:resolve-plugins dependency:go-offline

# RUN mvn -o compile

# RUN mvn -B -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml dependency:resolve

FROM base as builder

COPY src ./src
RUN mvn compile

# FROM builder as tester
# RUN mvn test

FROM builder as packager
RUN mvn package

FROM flink:1.13.6 as flinkbase

RUN mkdir -p $FLINK_HOME/usrlib
COPY --from=packager /app/target/beam-app-bundled-1.0.0.jar $FLINK_HOME/usrlib/beam-app-bundled-1.0.0.jar

