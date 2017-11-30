package net.foreach.data.utils.args;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Arguments parser class. Implemented to validate the input parameters given by the user. More
 * information in builder method.
 */
public class OrdersArgsParser {

    // Comma separated list of kafka brokers
    private static String DEFAULT_BROKERS = "localhost:9092";

    // Kafka topic
    private static String DEFAULT_TOPIC = "orders";

    // Url of schema registry server
    private static String DEFAULT_SCHEMA_REGISTRY = "";

    // Number of messages to generate per second, 0 means no limit
    private static int DEFAULT_TPS = 0;

    // Duration of loading process, 0 means non-stop
    private static long DEFAULT_DURATION = 60 * 1000L;

    public static final String CLI_CMD= "java -jar target/xxxx.jar "
        + "--brokers localhost:9092 "
        + "--topic orders "
        + "--schemaRegistry localhost:8081 "
        + "--tps 100 "
        + "--duration 60 ";

    private String brokers;
    private String topic;
    private String schemaRegistry;
    private int tps;
    private long duration;

    public OrdersArgsParser() {
    }


    /**
     * Command-line parser method. The application should accept some parameters:
     * 	1. A list of kafka brokers (comma separated)
     * 	2. A kafka topic name
     * 	3. A schema registry url (Optional)
     * 	4. The number of messages to send per second (Optional, no limit by default)
     * 	5. The duration of the loading process (Optional, 60 sec)
     *
     * @param args
     * @throws ParseException
     */
    public static OrdersArgsParser builder(String[] args) throws ParseException {
        Options options=getDefaultOptions();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String kafkaBrokers= cmd.getOptionValue("b", DEFAULT_BROKERS);
        String kafkaTopic = cmd.getOptionValue("t", DEFAULT_TOPIC);
        String schemaRegistryUrl = cmd.getOptionValue("s", DEFAULT_SCHEMA_REGISTRY);

        int tps;
        long duration;

        try {
            tps= Integer.parseInt(cmd.getOptionValue("m", String.valueOf(DEFAULT_TPS)));
        } catch (NumberFormatException ex)  {
            throw new ParseException("Invalid transactions per second (tps) given as parameter");
        }

        try {
            duration= Long.parseLong(cmd.getOptionValue("d", String.valueOf(DEFAULT_DURATION)));
        } catch (NumberFormatException ex)  {
            throw new ParseException("Invalid duration given as parameter");
        }

        try {
            OrdersArgsParser argsParser = new OrdersArgsParser()
                .setBrokers(kafkaBrokers)
                .setTopic(kafkaTopic)
                .setSchemaRegistry(schemaRegistryUrl)
                .setTps(tps)
                .setDuration(duration);
            return argsParser;

        } catch (Exception e) {
            throw new ParseException("Bad parameters used: " + args.toString());
        }

   }

    public static Options getDefaultOptions()   {
        Options options= new Options();

        options.addOption("b", "brokers", true, "Comma separated list of kafka brokers (ex: localhost:9092)");
        options.addOption("t", "topic", true, "Kafka topic where the loader will produce the messages");
        options.addOption("s", "schemaRegistry", true, "Schema registry url (ex: http://localhost:8081)");
        options.addOption("m", "tps", true, "The number of messages to send per second (0 means no limit)");
        options.addOption("d", "duration", true, "The duration of the loading process (0 means non-stop");

        return options;
    }

    @Override
    public String toString() {
        return "OrdersArgsParser{" +
            "brokers='" + brokers + '\'' +
            ", topic='" + topic + '\'' +
            ", schemaRegistry='" + schemaRegistry + '\'' +
            ", tps=" + tps +
            ", duration=" + duration +
            '}';
    }

    public String getBrokers() {
        return brokers;
    }

    public OrdersArgsParser setBrokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public OrdersArgsParser setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getSchemaRegistry() {
        return schemaRegistry;
    }

    public OrdersArgsParser setSchemaRegistry(String schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        return this;
    }

    public int getTps() {
        return tps;
    }

    public OrdersArgsParser setTps(int tps) {
        this.tps = tps;
        return this;
    }

    public long getDuration() {
        return duration;
    }

    public OrdersArgsParser setDuration(long duration) {
        this.duration = duration;
        return this;
    }
}
