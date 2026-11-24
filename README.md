# andres-cw

Spring Boot 4.2.0 baseline on Java 25 (Maven).

## Run

```
./mvnw spring-boot:run
```

On Windows PowerShell use `.\mvnw.cmd spring-boot:run`. The app listens on port 8080.

Health check: <http://localhost:8080/actuator/health>

## Build and test

```
./mvnw verify
java -jar target/andres-cw-0.0.1-SNAPSHOT.jar
```
