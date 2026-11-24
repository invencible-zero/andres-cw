# andres-cw

Spring Boot 4.2.0 baseline on Java 25 (Maven).

## Run

```
./mvnw spring-boot:run
```

On Windows PowerShell use `.\mvnw.cmd spring-boot:run`. The app listens on port 8080.

Health check: <http://localhost:8080/actuator/health>

## Database

In-memory H2 (`jdbc:h2:mem:andres_cw`), recreated on every start. Liquibase builds the schema on startup, so no manual SQL is needed.

- Master changelog: `src/main/resources/db/changelog/db.changelog-master.yaml`
- Add a schema change: create `db/changelog/changes/NNN-description.yaml` and `include` it in the master file. Never edit a changeset that has already run; add a new one.
- Applied changesets are recorded in the `DATABASECHANGELOG` table.
- Datasource and Liquibase settings are in `application.properties`.
- Because H2 is in-memory, data is lost on restart. Swapping to a persistent database means changing `spring.datasource.*` and the driver dependency; the changelogs stay as they are.

## Build and test

```
./mvnw verify
java -jar target/andres-cw-0.0.1-SNAPSHOT.jar
```
