package com.jeancsil.kafka;

import com.jeancsil.kafka.conf.BootstrapServer;
import com.jeancsil.kafka.producer.KafkaCountryProducer;
import com.jeancsil.models.CountryOuterClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Main {
  public static void main(String[] args) {
    final var bootstrapServers = new BootstrapServer("0.0.0.0:9092");

    final var topic = "countries";
    final var producer = new KafkaCountryProducer(List.of(bootstrapServers)).getProducer();

    System.out.println("Started ...");
    log.info("Started ...");
    var geonameId = 100;
    var name = "Brasil";

    final var country = CountryOuterClass.Country
            .newBuilder()
            .setName(name)
            .setGeonameId(geonameId)
            .build();

    final var record = new ProducerRecord<Integer, CountryOuterClass.Country>(
            topic,
            country);

    try {
      RecordMetadata metadata = producer.send(record).get();
      System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
              record.key(), record.value(), metadata.partition(),
              metadata.offset());
    } catch (InterruptedException| ExecutionException e) {
      log.error(e.getMessage(), e);
    } finally{
      producer.flush();
      producer.close();
    }
    System.out.println("Finished...");
  }
}
