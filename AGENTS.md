# SampleSink

Plugin de CDAP (sink de Spark) que consume registros de entrada, escribe una muestra en el log y no almacena nada.

## Visibilidad y rama

- Público (licencia Apache 2.0). Rama por defecto: `main`.

## Stack

- Java 1.8 (`maven-compiler-plugin` con `source`/`target` 1.8), Maven.
- CDAP 6.4.1 (`cdap.version`), Spark 2.1.3 (`spark.version`), logback 1.0.9 (solo tests).
- Dependencias clave en `pom.xml`: `cdap-api`, `cdap-etl-api`, `cdap-etl-api-spark`, `cdap-api-spark2_2.11`, `spark-repl_2.11` (todas `provided`), `hydrator-common` 2.4.0, `cdap-formats`.
- Plugins de Maven: `maven-bundle-plugin` (empaqueta dependencias en `lib`), `cdap-maven-plugin` 1.1.0 (genera la config del artefacto en `prepare-package`).

## Estructura

- `src/main/java/org/toletum/cdap/plugin/SampleSink.java`: clase del plugin (`@Plugin`, `@Name("SampleSink")`), con `run()` que hace `take(sampleNumber)` y escribe en el log.
- `widgets/SampleSink-sparksink.json`: definición de la UI del plugin (propiedad `sampleNumber`).
- `docs/SampleSink-sparksink.md`: documentación del plugin.
- `pom.xml`: dependencias, build y propiedades de CDAP.
- `README.md` y `LICENSE`.

## Comandos

- Compilar y empaquetar: `mvn package` (según `pom.xml`; no verificado).
- Tests: `mvn test` (surefire incluye `*Test.java` y `*TestSuite.java`, pero no hay tests en el repo).
- Despliegue en CDAP: no documentado en el repo.

## Configuración

- Propiedad del plugin: `sampleNumber` (entero, admite macros `${...}`), número de registros a mostrar.
- No hay variables de entorno.

## Convenciones para agentes

- Respetar las versiones de CDAP/Spark del `pom.xml`: el plugin depende de APIs de CDAP 6.x y de Spark 2.x (Scala 2.11).
- Si se cambia `sampleNumber` o sus propiedades, actualizar también `widgets/SampleSink-sparksink.json` y `docs/`.
- Mantener el encabezado de licencia Apache en los ficheros Java.

## Estado

- Sin tests: `src/test` no existe.
- El README dice que el plugin "log 100" registros, pero el código usa `sampleNumber` configurable: la documentación está desactualizada.
- `docs/SampleSink-sparksink.md` dice que el plugin "se come" todos los registros; el código solo muestra una muestra con `take()`.
- `SampleSink.java`: código sin uso (`Iterator<Schema.Field> fieldIter`, varios imports sin usar, `@SuppressWarnings("unused")`).
- Se usa `classBuildTimeMillis` para registrar la fecha de compilación en `prepareRun`.
