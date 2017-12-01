# data-models

Library including data models generated using avro schemas. You can find the available models in src/main/resources folder.

To generate the models run:

```mvn clean package``` 

Execute a install if you need to install in your local repository.

You can import the beans generated adding the following dependency to your project:
```
        <dependency>
            <groupId>net.foreach.data</groupId>
            <artifactId>models</artifactId>
            <version>0.0.1</version>
        </dependency>
```

kafka-topics --list --zookeeper zookeeper:2181
 kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
 

