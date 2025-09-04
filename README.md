# kafka-topic-compare

kafka-topic-compare is a tool for comparing two Kafka topics. It detects differences in messages, including keys, values, and headers. It reports:
- Messages only in topic A or B
- Duplicate messages in either topic
- Messages with the same key and value but different headers

## CLI Usage

After building the project, you can run the comparison from the command line:

```sh
java -jar target/quarkus-app/quarkus-run.jar \
  --bootstrapA localhost:9092 --topicA topicA \
  --bootstrapB localhost:9093 --topicB topicB \
  --maxMessages 100
```

### Arguments
- `--bootstrapA` Kafka bootstrap servers for topic A
- `--topicA` Name of topic A
- `--bootstrapB` Kafka bootstrap servers for topic B
- `--topicB` Name of topic B
- `--maxMessages` Maximum number of messages to compare from each topic

### Example Output
```
ONLY_IN_A: key=null value=foo
ONLY_IN_B: key=null value=bar
DUPLICATE_IN_A: key=abc value=xyz
HEADER_DIFFERENCE: key=abc value=xyz headersA={foo=bar} headersB={foo=baz}
```

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/kafka-topic-compare-1.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/maven-tooling>.

## Related Guides

- Apache Kafka Client ([guide](https://quarkus.io/guides/kafka)): Connect to Apache Kafka with its native API
