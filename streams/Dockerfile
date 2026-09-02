FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

# Dependencies first: a source change should not re-resolve the whole tree.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package

# A glibc base, not Alpine, and the reason is worth recording.
#
# The Go producers compress with Snappy. Kafka's Java client decompresses it
# through org.xerial.snappy, which loads a native library linked against
# glibc — so on a musl base every fetch of a compressed batch dies with an
# UnsatisfiedLinkError, at runtime, only once real data arrives. Nothing about
# the build or the tests catches it.
#
# The alternative was to drop the whole pipeline to gzip or lz4, which have
# pure-Java implementations. Choosing the pipeline's wire compression to suit
# one container's libc is the wrong way round; ninety megabytes of base image
# is the cheaper concession.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/vigil-streams.jar /app/vigil-streams.jar
RUN useradd --system --create-home --uid 10001 vigil
USER vigil
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/vigil-streams.jar"]
