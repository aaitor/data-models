package net.foreach.data.loader;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import net.foreach.data.models.avro.Line;
import net.foreach.data.models.avro.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import net.foreach.data.utils.SpecificAvroSerializer;

/**
 * Created by aitor on 25/6/17.
 */
public class RunLoader {

    private static String bootstrapServers= "localhost:9092";
    private static String schemaRegistryUrl= "http://localhost:8081";
    private static long duration= 60 * 1000L;


    private static Properties props = new Properties();
    private static CachedSchemaRegistryClient schemaRegistry;
    private static Map<String, String> serdeProps;
    private static SpecificAvroSerializer<Order> orderSerializier;

    private static long WAIT= 100L;
    public static final String ORDERS_TOPIC = "orders";

    private static final Logger log = Logger.getLogger(RunLoader.class);

    public static void main(String [] args) throws Exception {

        log.info("Initializing RunLoader class");

        if (args.length> 0)
            bootstrapServers= args[0];

        if (args.length> 1)
            schemaRegistryUrl= args[1];

        if (args.length> 2)
            duration= Long.parseLong(args[2]) * 1000L;

        initializeConfig();

        final KafkaProducer<String, Order> orderProducer = new KafkaProducer<>(props,
                Serdes.String() .serializer(),
                orderSerializier);

        run(orderProducer, duration);
    }

    private static void run(KafkaProducer<String, Order> producer,long duration) throws InterruptedException {
        while (true) {
            producer.send(new ProducerRecord<>(ORDERS_TOPIC, "", new Order()));
            Thread.sleep(WAIT);
        }
    }

    private static void initializeConfig()  {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        schemaRegistry= new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        serdeProps= Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        orderSerializier = new SpecificAvroSerializer<>(schemaRegistry, serdeProps);
        orderSerializier.configure(serdeProps, false);

    }

}
