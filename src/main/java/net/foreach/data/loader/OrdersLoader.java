package net.foreach.data.loader;

import static java.nio.charset.Charset.forName;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.benas.randombeans.EnhancedRandomBuilder;
import io.github.benas.randombeans.api.EnhancedRandom;
import java.time.LocalDate;
import net.foreach.data.models.avro.Order;
import net.foreach.data.utils.Randomizer;
import net.foreach.data.utils.args.OrdersArgsParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import net.foreach.data.utils.SpecificAvroSerializer;

/**
 * Created by aitor on 25/6/17.
 */
public class OrdersLoader {


  private static final Logger log = Logger.getLogger(OrdersLoader.class);

    protected Properties props = new Properties();
    protected CachedSchemaRegistryClient schemaRegistry;
    protected Map<String, String> serdeProps;
    protected SpecificAvroSerializer<Order> orderSerializier;
    protected KafkaProducer<String, Order> producer;
    protected static long WAIT= 1000L;


    private OrdersLoader() {
    }


    private void run(KafkaProducer<String, Order> producer, String topic, int tps, long duration) throws InterruptedException {

        boolean continueRunning= true;
        long startTime= System.currentTimeMillis();
        long timeElapsed= 0L;
        int transactionsCounter= 0;

        log.info("Starting Producer");

        Order order= null;
        ProducerCallback callback= new ProducerCallback();

        while (continueRunning) {
            order= Randomizer.getRandomOrder();
            if (tps >0) {
                if (transactionsCounter< tps) {
                    log.info("Producing Order (t:" + transactionsCounter + ") = " + order.getOrderId());
                    ProducerRecord data= new ProducerRecord<>(topic, order);
                    producer.send(data, callback);
                    transactionsCounter++;
                }   else    {
                    log.info("TPS cap reached. Waiting ....");
                    transactionsCounter= 0;
                    Thread.sleep(WAIT);
                }
            }   else {
                log.info("Producing Order = " +  order.getOrderId());
                //producer.send(new ProducerRecord<>(topic, order.getOrderId(), order), callback);
                ProducerRecord data= new ProducerRecord<>(topic, order);
                producer.send(data, callback);
            }

            timeElapsed= System.currentTimeMillis() - startTime;
            if (duration >0 && (timeElapsed >= duration * 1000L)) {
                log.info("Time elaspsed: " + timeElapsed + "ms. Stopping producer");
                continueRunning = false;
            }
        }

        producer.close();

    }

    public static OrdersLoader builder(OrdersArgsParser argsParser)  {
        OrdersLoader loader= new OrdersLoader();
        loader.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, argsParser.getBrokers());
        loader.props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Key and Value serializers (String and Avro)
        loader.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        if (!argsParser.getSchemaRegistry().isEmpty()) {
            loader.schemaRegistry = new CachedSchemaRegistryClient(argsParser.getSchemaRegistry(), 100);
            loader.serdeProps = Collections.singletonMap("schema.registry.url", argsParser.getSchemaRegistry());
            loader.orderSerializier = new SpecificAvroSerializer<>(loader.schemaRegistry, loader.serdeProps);
            loader.orderSerializier.configure(loader.serdeProps, false);
            loader.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        }   else    {
            loader.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        }

        loader.producer = new KafkaProducer<>(
            loader.props,
            Serdes.String().serializer(),
            loader.orderSerializier);

        return loader;
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                log.error("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("Sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                log.info(message);
            }
        }
    }


    public static void main(String [] args) throws Exception {

        log.info("Initializing OrdersLoader");
        OrdersArgsParser argsParser= null;
        OrdersLoader loader= null;

        try {
            log.debug("Parsing input parameters");
            argsParser= OrdersArgsParser.builder(args);
            loader= OrdersLoader.builder(argsParser);
        } catch (ParseException ex)	{
            log.error("Unable to parse arguments");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(OrdersArgsParser.CLI_CMD, OrdersArgsParser.getDefaultOptions());
            System.exit(1);
        }

        log.info("Initializing loader with the following parameters:" + argsParser.toString());
        loader.run(loader.producer, argsParser.getTopic(), argsParser.getTps(), argsParser.getDuration());
    }

}
