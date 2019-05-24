FROM openjdk
FROM maven
WORKDIR /app
COPY src /app/src
COPY pom.xml /app
RUN mkdir models
COPY ../models models
RUN mvn clean package
CMD java -jar /app/target/udp-actor-1.0-SNAPSHOT.jar
