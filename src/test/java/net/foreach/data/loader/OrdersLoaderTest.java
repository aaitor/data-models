package net.foreach.data.loader;

import static org.junit.Assert.*;

import net.foreach.data.utils.Randomizer;
import net.foreach.data.utils.args.OrdersArgsParser;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OrdersLoaderTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void builderTest() throws Exception {
        String args[]= {};
        OrdersArgsParser parser= OrdersArgsParser.builder(args);

        OrdersLoader loader= OrdersLoader.builder(parser);

        assertEquals("localhost:9092", loader.props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void runnerTest() throws Exception {

    }

}