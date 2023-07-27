FROM ubuntu:rolling

RUN apt-get update && apt-get install -y openjdk-17-jdk

COPY target/googlecloud-1.0-SNAPSHOT.jar ggooglecloud-1.0-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "/googlecloud-1.0-SNAPSHOT.jar"]