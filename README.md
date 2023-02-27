package com.bhaskar.drouter;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class TopologyProducer {
    private static final String ORDER_TOPIC = "order";
    private static final String ORDER_OUTPUT= "order-topic";
    //private static final String PRODUCT_TOPIC = "product";


    @Produces
    public Topology getTopCharts() {

        final StreamsBuilder builder = new StreamsBuilder();


        final ObjectMapperSerde<Order> OrderSerder = new ObjectMapperSerde<>(Order.class);


        builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Integer(), OrderSerder))
                .filter((value, order) -> "bhaskar".equals(order.getProductName()))
                .to(ORDER_OUTPUT, Produced.with(Serdes.Integer(), OrderSerder));

        return builder.build();

    }
}
