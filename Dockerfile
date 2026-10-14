# 1. Етап на компилация
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# 2. Етап на изпълнение
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

COPY *.html *.js *.css ./

# Казваме на Railway, че ще ползваме стандартния порт 8080 (или този от системата)
ENV PORT=8080
EXPOSE 8080

CMD ["java", "-jar", "app.jar"]
