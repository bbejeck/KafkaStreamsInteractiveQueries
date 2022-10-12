## Interactive Queries in Kafka Streams

This repo contains the source code for the "Building Interactive Query Service" presentation at Current 2022 in Austin,
Texas on October 5th 2022.

### About the project

The goal of this GitHub repo is to server as a basic reference implementation for building an interactive query service
in Kafka Streams. For building your own IQ application you should be able to fork this project and follow the pattern
here for adding your own controllers and web page for displaying the results. This project also uses Standby tasks for demonstrating high-availability for Kafka Streams **_as well as_** interactive queries.

### Dependencies

The project uses the following dependencies

* For the web serving layer the application uses [Spring Boot](http://spring-boot-url) Note that we're only using Spring Boot for providing the dependency injection required to wire up the application - no specific `spring-kafka` or `spring-kafka-streams` functionality is leveraged like `@EnableKafkaStreams`
* The application is using [JQuery](http://jquery) for making REST API calls and rendering the results in a dynamic web page found in [](src/main/resources/public/index.html)
* The integration tests use [Testcontainers](http://testcontainers/url) and [Junit 5](http://junit5/url). The integration tests don't use any spring specific testing utilities

### Running the application 

To run the application you have two options

1. COMING SOON: Build an uber jar from the project by running `./gradlew shadowJar` then run `java -jar kafka-streams-iq-app.jar` from the command line.  Note to run more than one instance on the same host, you'll have to set a unique port for each instance using a `-Dserver.port=NNNN` setting for each one.
2. Run from IntelliJ.  Again you'll have to set a unique port for running more than one instance using `-Dserver.port=NNNN` in the `Spring-Boot` section of the `run/debug` configuration settings
3. To push records through the Kafka Streams application run the [TestDataProducer](src/main/java/io/confluent/developer/streams/TestDataProducer.java) class again from either IntelliJ or a separate command from the command line `java -jar jarFile TestDataProducer`
4. The [application.properties](src/main/resources/application.properties) file contains the Kafka Streams properties. The configuration class [KafkaStreamsAppConfiguration](src/main/java/io/confluent/developer/config/KafkaStreamsAppConfiguration.java) class ingests these properties. For connecting to [Confluent Cloud](https://confluent-cloud.io) there's the [confluent.properties.orig](src/main/resources/confluent.properties.orig) you can also use this file for secure connections to any Kafka cluster. The [KafkaStreamsAppConfiguration](src/main/java/io/confluent/developer/config/KafkaStreamsAppConfiguration.java) class is expecting a `confluent.properties` file to exist to use for secure connections. To make sure you don't check in any sensitive configs save the `confluent.properties.orig` file as `confluent.properties` which is set to ignore in the `.gitignore` file.
5. There is a configuration `secure.configs` in the [application.properties](src/main/resources/application.properties) file which is set to `true` by default - to disable loading the `confluent.properties` file (testing, or on-prem unsecure brokers) set this configuration to `false`

### Testing

There is an integration test for both key and range query:
[InteractiveQueriesIntegrationTest](src/test/java/io/confluent/developer/InteractiveQueriesIntegrationTest.java) For both tests, they start two Kafka Streams applications, produces some records then asserts both active hosts return results then shuts down one application and asserts that the standby returns query results. The integration tests serve to show an example of testing an IQ application and will also help validate any changes you may make in the application.

**NOTE**: that the test uses testcontainers and the Kafka image is for M1 macs so if you're on `x86` you'll have to adjust the docker image pulled for the test.



