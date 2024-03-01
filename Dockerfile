FROM docker.io/maven:3.8-jdk-11 as builder
FROM docker.io/adoptopenjdk/openjdk11:alpine-jre
WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY /target/com.kannappan.compare.data.count.jar /com.kannappan.compare.data.count.jar
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/com.kannappan.compare.data.count.jar", "--spring.profiles.active=${KANNAPPAN_ENV}"]
