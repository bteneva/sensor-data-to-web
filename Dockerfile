# 1. Етап на компилация
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# 2. Етап на изпълнение
FROM eclipse-temurin:17-jre
WORKDIR /app
# Копираме компилирания .jar файл от първия етап
COPY --from=build /app/target/*.jar app.jar
# Стартираме приложението
CMD ["java", "-jar", "app.jar"]
