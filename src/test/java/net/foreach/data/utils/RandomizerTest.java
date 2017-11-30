package net.foreach.data.utils;

import static org.junit.Assert.*;

import net.foreach.data.models.avro.Order;
import org.junit.Test;

public class RandomizerTest {

    @Test
    public void getRandomOrder() throws Exception {
        Order order= Randomizer.getRandomOrder();

        assertTrue(order.getOrderId().length() >= 5);
        assertTrue( order.getPaymentMethod().toString().equals("CASH")
            || order.getPaymentMethod().toString().equals("CARD"));
        assertTrue( order.getOrderDatetime() > 1000);
        assertTrue( order.getLines().size() > 0);
    }

}