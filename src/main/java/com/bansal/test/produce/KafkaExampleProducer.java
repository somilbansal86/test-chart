package com.bansal.test.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

/**
 * Generate input to Kafka topic.
 * Example (key,value) messages:
 * (PG,120)
 * (HX,111)
 * (PG,105)
 * (OY,121)
 *
 * key - represents LSF cluster name
 * value - represents waiting time for particular job submission
 *
 */
public class KafkaExampleProducer {

    public static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        String[] clusterNames = {"YHOO"};
        Random random = new Random();


        while (true) {

           // for (String clusterName : clusterNames) {

               // double ifSkip = random.nextInt(10);

               // if (clusterName.equals("YHOO") && ifSkip < 2) continue;
                /*if (clusterName.equals("GOOG") && ifSkip < 4) continue;
                if (clusterName.equals("AAPL") && ifSkip < 6) continue;
                if (clusterName.equals("IBM") && ifSkip < 8) continue;*/

                int waitTime = 0;

              //  if (clusterName.equals("YHOO")) waitTime = random.nextInt(30) + 100;
             /*   if (clusterName.equals("GOOG")) waitTime = random.nextInt(30) + 110;
                if (clusterName.equals("AAPL")) waitTime = random.nextInt(30) + 120;
                if (clusterName.equals("IBM")) waitTime = random.nextInt(30) + 130;*/

                int wait1 = 110;
                int wait2 = 120;
                int wait3 = 130;

                ProducerRecord<String, String> record = new ProducerRecord<>("data-in", "YHOO", String.valueOf(wait1));
                ProducerRecord<String, String> record2 = new ProducerRecord<>("data-in", "YHOO", String.valueOf(wait2));
                ProducerRecord<String, String> record3 = new ProducerRecord<>("data-in", "YHOO", String.valueOf(wait3));

                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing to topic " + r.topic());
                        e.printStackTrace();
                    }
                });
                Thread.sleep(2001);
                producer.send(record2, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing to topic " + r.topic());
                        e.printStackTrace();
                    }
                });
                Thread.sleep(2001);
                producer.send(record3, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing to topic " + r.topic());
                        e.printStackTrace();
                    }
                });
                Thread.sleep(2001);
            //}
        }
    }
}
