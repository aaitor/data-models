package net.foreach.data.utils.args;

import static org.junit.Assert.*;

import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OrdersArgsParserTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void simpleBuilderTest() throws Exception {
        String args[]= {
            "--brokers", "kafka:9092",
            "--topic", "test-topic",
            "--schemaRegistry", "registry:8081",
            "--tps", "100",
            "--duration", "60"
        };

        OrdersArgsParser parser= OrdersArgsParser.builder(args);
        assertEquals("kafka:9092", parser.getBrokers());
        assertEquals("registry:8081", parser.getSchemaRegistry());
        assertEquals("test-topic", parser.getTopic());
        assertEquals(100, parser.getTps());
        assertEquals(60L, parser.getDuration());

    }

    @Test
    public void defaultParamsBuilderTest() throws Exception {
        String args[]= {};

        OrdersArgsParser parser= OrdersArgsParser.builder(args);
        assertEquals("localhost:9092", parser.getBrokers());
        assertEquals("orders", parser.getTopic());
        assertEquals("", parser.getSchemaRegistry());
        assertEquals(0, parser.getTps());
        assertEquals(60L * 1000L, parser.getDuration());

    }

    @Test(expected = ParseException.class)
    public void invalidTpsBuilderTest() throws Exception {
        String args[]= {"--tps", "fds"};

        OrdersArgsParser.builder(args);


    }
}