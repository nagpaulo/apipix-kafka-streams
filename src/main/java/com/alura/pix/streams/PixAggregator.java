package com.alura.pix.streams;

import com.alura.pix.avro.PixRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;

@Service
public class PixAggregator {
    @Autowired
    public void aggregator(StreamsBuilder streamsBuilder, @Qualifier("schemaRegistry") String schemaRegistry) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistry);

        final Serde<String> keyAvroSerde = Serdes.String();
        keyAvroSerde.configure(serdeConfig, true);
        final Serde<PixRecord> pixAvroSerde = new SpecificAvroSerde<>();
        pixAvroSerde.configure(serdeConfig, false);

        KStream<String, PixRecord> messageStream = streamsBuilder
                .stream("astin04.poc-pix-topic", Consumed.with(keyAvroSerde, pixAvroSerde))
                .peek((key, value) -> System.out.println("Pix recebido (Streams) (filter): " + value.getChaveOrigem()))
                .filter((key, value) -> value.getValor() > 1000)
                .peek((key, value) -> System.out.println("Pix: " + key + " será verificado para possível frause"));

        messageStream.print(Printed.toSysOut());
        messageStream.to("astin04.poc-pix-verificacao-fraude", Produced.with(keyAvroSerde, pixAvroSerde));

        KTable<String, Double> aggregateStream = streamsBuilder
                .stream("astin04.poc-pix-topic", Consumed.with(keyAvroSerde, pixAvroSerde))
                .peek((key, value) -> System.out.println("Pix recebido (Streams) (Aggregatos): " + value.getChaveOrigem()))
                .filter((key, value) -> !Double.isNaN(value.getValor()))
                .groupBy((key, value) -> value.getChaveOrigem().toString())
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> (aggregate + value.getValor()),
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        aggregateStream.toStream().print(Printed.toSysOut());
    }
}
