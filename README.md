# kafka-topic-compare

A powerful tool for comparing or diff two Kafka topics, designed for developers and operators who need to verify data consistency between Kafka clusters or topics.

## Features
- Compares two Kafka topics for differences in messages, keys, values, and headers
- Detects:
  - Messages only in topic A or B
  - Duplicate messages in either topic
  - Messages with the same key and value but different headers
  - Out-of-order messages
- CLI tool, easy to run locally or in CI/CD
- Supports large topics and configurable message limits
- Support compacted topics where we check that the latest key versions / timestamps match
- Automated tests and dependency updates via GitHub Actions and Dependabot
- Log differences in topic properties

## Installation

**Requirements:**
- Java 17 or newer
- Maven (or use the included `mvnw` wrapper)

Clone the repository and build the project:

```sh
./mvnw clean package
```

The built JAR will be in `target/quarkus-app/quarkus-run.jar`.

## Usage

Run the comparison tool from the command line:

```sh
java -jar target/quarkus-app/quarkus-run.jar \
  --bootstrapA localhost:9092 --topicA topicA \
  --bootstrapB localhost:9093 --topicB topicB \
  --maxMessages 100
```

This will run a comparison of `topicA` on the Kafka cluster at `localhost:9092` with `topicB` on the Kafka cluster at `localhost:9093`, comparing up to 100 messages from the beginning of each topicpartition.
Since offsets may not align between topics, the tool matches messages by key and timestamp. You may also want to set the starting timestamp `--startTimestamp` to avoid comparing old messages.

### Arguments
- `--bootstrapA` Kafka bootstrap servers for topic A (default: `localhost:9092`)
- `--topicA` Name of topic A (default: `topicA`)
- `--bootstrapB` Kafka bootstrap servers for topic B (default: `localhost:9093`)
- `--topicB` Name of topic B (default: `topicB`)
- `--maxMessages` Maximum number of messages to compare from each topic (default: `1000`)
- `--clientPropertiesA` (optional) Path to a Java properties file for topic A consumer configuration
- `--clientPropertiesB` (optional) Path to a Java properties file for topic B consumer configuration
- `--output` or `-o` Output format: `csv` (default) or `json`
- `--startTimestamp` (optional) Only compare messages with timestamp >= this ISO-8601 value or epoch milliseconds
- `--print-diff` Print detailed differences for messages with the same key but different values/headers
- `--skip-header` Optional comma-separated list of header names to skip in diff (or empty to disable header comparison)
- `--skip-missing-at-end` Skip logging/reporting of differences of type MISSING_AT_END`
- `--debug` Enable debug logging
- `--help` Show help and exit

> For advanced Kafka consumer settings (e.g., group.id, deserializers), use `--clientPropertiesA` and `--clientPropertiesB` to provide a properties file. CLI arguments take precedence over properties file values.
 


NOTE: The outputs of the tool are sent to standard out (stdout). Errors and logs are sent to standard error (stderr).
If you want to redirect output to a file, use shell redirection, e.g.:
```sh
# logs into error.log, output into output.csv
java -jar target/quarkus-app/quarkus-run.jar ... 1>output.csv 2>error.log

# Output and logs into the same file
java -jar target/quarkus-app/quarkus-run.jar ... &> output.log
```

### Example Output
#### CSV (default)
```
type,bootstrapA,topicA,partitionA,offsetA,bootstrapB,topicB,partitionB,offsetB
ONLY_IN_A,localhost:9092,topicA,0,10,localhost:9093,topicB,,
ONLY_IN_B,localhost:9092,topicA,,,localhost:9093,topicB,1,15
HEADER_DIFFERENCE,localhost:9092,topicA,0,12,localhost:9093,topicB,1,17
```
#### JSON
```
{"type":"ONLY_IN_A","bootstrapA":"localhost:9092","topicA":"topicA","partitionA":0,"offsetA":10,"bootstrapB":"localhost:9093","topicB":"topicB","partitionB":null,"offsetB":null}
{"type":"ONLY_IN_B","bootstrapA":"localhost:9092","topicA":"topicA","partitionA":null,"offsetA":null,"bootstrapB":"localhost:9093","topicB":"topicB","partitionB":1,"offsetB":15}
{"type":"HEADER_DIFFERENCE","bootstrapA":"localhost:9092","topicA":"topicA","partitionA":0,"offsetA":12,"bootstrapB":"localhost:9093","topicB":"topicB","partitionB":1,"offsetB":17}
```

## Reproduction

In ordre to reproduce a case where two topics are different, you can use the provided `demo` folder. 
This includes a `docker-compose.yml` file to start two Kafka clusters and a mirror maker setup to replicate data between them.

```bash
# ensure you have built the application
./mvnw package
 
# Start the Kafka clusters and MirrorMaker
cd demo
docker-compose up -d

# wait for datagen to be started
while [ "$(curl -s -o /dev/null -w "%{http_code}" localhost:8083/health)" != "200" ]; do     sleep 1; done

# wait until everything is started and data is replicated
# list topics in both clusters
docker exec -it brokerA /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9091 --list
docker exec -it brokerB /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# verify you see `users` topic in both clusters

# note the current timestamp in milliseconds
TS=$(date +%s%3N) && echo $TS

# kill the mirror maker to stop replication
docker compose kill mirror-maker

# run the comparison tool to see that topics are identical
java -jar target/quarkus-app/quarkus-run.jar \
  --bootstrapA localhost:9091 \
  --topicA users \
  --bootstrapB localhost:9092 \
  --topicB users \
  --maxMessages 1000 \
  --startTimestamp $TS  
```

You will see that there is events at the end of topic A that are not in topic B, because the mirrormaker was killed. e.g. `MISSING_AT_END,localhost:9091,users,4,225,localhost:9092,,,`

```bash
# Now produce some new data into topic B
echo 'some-key,random message' | docker exec -i brokerB /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic users --property parse.key=true --property key.separator=,

# run the comparison tool again

java -jar ../target/quarkus-app/quarkus-run.jar \
  --bootstrapA localhost:9091 \
  --topicA users \
  --bootstrapB localhost:9092 \
  --topicB users \
  --maxMessages 1000 \
  --startTimestamp $TS
```

You will see that there is now a message in topic B that is not in topic A `ONLY_IN_B,localhost:9091,,,,localhost:9092,users,2,73`

```bssh
# restart mirrormaker
docker compose up -d mirror-maker

# run the comparison tool again after some time
java -jar ../target/quarkus-app/quarkus-run.jar \
  --bootstrapA localhost:9091 \
  --topicA users \
  --bootstrapB localhost:9092 \
  --topicB users \
  --maxMessages 1000 \
  --startTimestamp $TS
```

You will see that there are some duplicates (in most cases) and and after some events the messages are identical again.


```csv
type,bootstrapA,topicA,partitionA,offsetA,bootstrapB,topicB,partitionB,offsetB
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,1,207
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,1,208
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,2,79
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,5,93
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,5,94
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,5,95
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,0,174
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,0,175
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,4,234
DUPLICATE_IN_B,localhost:9091,,,,localhost:9092,users,4,235

```




## Running Tests

To run all tests:
```sh
./mvnw test
```

## Running in Development Mode

You can run your application in dev mode with live coding using:
```sh
./mvnw quarkus:dev
```

Dev UI is available at <http://localhost:8080/q/dev/> in dev mode.

## Packaging and Running

To build the application:
```sh
./mvnw package
```

The application is runnable using:
```sh
java -jar target/quarkus-app/quarkus-run.jar
```

If you want to build an _über-jar_:
```sh
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

Run with:
```sh
java -jar target/*-runner.jar
```

## Building a Native Executable

With GraalVM installed:
```sh
./mvnw package -Dnative
```
Or using a container:
```sh
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

Run the native executable:
```sh
./target/kafka-topic-compare-1.0-SNAPSHOT-runner
```

## CI/CD and Dependency Management
- **CI:** All pull requests are automatically tested via GitHub Actions.
- **Release:** JARs are built and attached to releases when a tag is pushed to the `main` branch.
- **Dependabot:** Keeps Maven and Docker dependencies up to date, grouping updates for easier review.

## Related Guides
- [Apache Kafka Client](https://quarkus.io/guides/kafka): Connect to Apache Kafka with its native API

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
