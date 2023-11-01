package com.alura.pix.streams;

import com.alura.pix.dto.PixDTO;
import com.alura.pix.serdes.PixSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PixAggregator {
    @Autowired
    public void aggregator(StreamsBuilder streamsBuilder) {

        KStream<String, PixDTO> messageStream = streamsBuilder
                .stream("pix-topic", Consumed.with(Serdes.String(), PixSerdes.serdes()))
                .peek((key, value) -> System.out.println("Pix recebido (Streams) (filter): " + value.getChaveOrigem()))
                .filter((key, value) -> value.getValor() > 1000)
                .peek((key, value) -> System.out.println("Pix: " + key + " será verificado para possível frause"));

        messageStream.print(Printed.toSysOut());
        messageStream.to("pix-verificacao-fraude", Produced.with(Serdes.String(), PixSerdes.serdes()));

        KTable<String, Double> aggregateStream = streamsBuilder
                .stream("pix-topic-2", Consumed.with(Serdes.String(), PixSerdes.serdes()))
                .peek((key, value) -> System.out.println("Pix recebido (Streams) (Aggregatos): " + value.getChaveOrigem()))
                .filter((key, value) -> value.getValor() != null)
                .groupBy((key, value) -> value.getChaveOrigem())
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> (aggregate + value.getValor()),
                        Materialized.with(Serdes.String(), Serdes.Double())
                );


        aggregateStream.toStream().print(Printed.toSysOut());
    }
}
