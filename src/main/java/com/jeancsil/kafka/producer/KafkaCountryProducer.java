package com.jeancsil.kafka.producer;

import com.jeancsil.kafka.conf.BootstrapServer;
import com.jeancsil.kafka.serializer.KafkaProtobufSerializer;
import com.jeancsil.models.CountryOuterClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaCountryProducer {

    private final KafkaProducer<Integer, CountryOuterClass.Country> producer;

    public KafkaCountryProducer(List<BootstrapServer> servers) {
        producer = createProducer(servers);
    }

    public KafkaProducer<Integer, CountryOuterClass.Country> getProducer() {
        return producer;
    }

    private KafkaProducer<Integer, CountryOuterClass.Country> createProducer(List<BootstrapServer> servers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", Arrays.asList(servers.stream().map(BootstrapServer::host).collect(Collectors.joining()))));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCountryProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        return new KafkaProducer<>(props);
    }
}
