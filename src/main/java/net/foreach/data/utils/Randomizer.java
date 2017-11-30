package net.foreach.data.utils;

import static java.nio.charset.Charset.forName;

import io.github.benas.randombeans.EnhancedRandomBuilder;
import io.github.benas.randombeans.api.EnhancedRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import net.foreach.data.models.avro.Order;

public abstract class Randomizer {

    public static EnhancedRandom randomizer= EnhancedRandomBuilder.aNewEnhancedRandomBuilder()
        .seed(123L)
        .objectPoolSize(100)
        .randomizationDepth(3)
        .charset(forName("UTF-8"))
        .dateRange(LocalDate.now().minusDays(1), LocalDate.now())
        .stringLengthRange(8, 16)
        .collectionSizeRange(1, 10)
        .scanClasspathForConcreteTypes(true)
        .overrideDefaultInitialization(false)
        .build();

    public static Order getRandomOrder()   {
        Order order= Randomizer.randomizer.nextObject(Order.class);
        order.setOrderDatetime(
            LocalDateTime.now().toEpochSecond(
                ZoneOffset.systemDefault().getRules().getOffset(Instant.now())));
        return order;
    }

}
