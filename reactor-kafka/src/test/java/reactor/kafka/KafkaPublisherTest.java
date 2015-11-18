package reactor.kafka;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.Firehose;
import reactor.pipe.key.Key;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KafkaPublisherTest {


  @Test
  public void kafkaPublisherTest() throws InterruptedException {
//    int iterations = 10;
//    CountDownLatch latch = new CountDownLatch(iterations);
//    String topic = makeTopicName();
//
//    Firehose<Key> publisherFirehose = makePublisher(topic);
//    Firehose<Key> consumerFirehose = makeConsumer(topic);
//
//
//    for (int i = 0; i < iterations; i++) {
//      consumerFirehose.notify(Key.wrap("kafkaKey"),
//                              "value " + i);
//    }
//
//    publisherFirehose.on(Key.wrap("kafkaKey"),
//                         new SimpleConsumer<Object>() {
//                           @Override
//                           public void accept(Object value) {
//                             System.out.println(value);
//                             latch.countDown();
//                           }
//                         });
//
//    latch.await(2, TimeUnit.SECONDS);
//    assertThat(latch.getCount(), is(0L));
//
  }


  public Firehose<Key> makePublisher(String topic) {
    Firehose<Key> publisherFirehose = new Firehose<>();
    KafkaPublisher<Key, String> kafkaPublisher = new KafkaPublisher<>(consumerProperties(),
                                                                      topic,
                                                                      (i) -> {
                                                                        return Key.wrap((Object[])new String(i).split(","));
                                                                      },
                                                                      String::new);
    Subscriber<Tuple2<Key, String>> firehoseConsumer = publisherFirehose.makeSubscriber();
    kafkaPublisher.subscribe(firehoseConsumer);
    return publisherFirehose;
  }

  public Firehose<Key> makeConsumer(String topic) {
    Firehose<Key> consumerFirehose = new Firehose<>();
    KafkaSubscriber<Key, String> kafkaSubscriber = new KafkaSubscriber<>(producerProperties(),
                                                                         topic,
                                                                         (i_) -> 0,
                                                                         System.out::println);

    Publisher<Tuple2<Key, String>> firehosePublisher = consumerFirehose.makePublisher(Key.wrap("kafkaKey"));
    firehosePublisher.subscribe(kafkaSubscriber);
    return consumerFirehose;
  }

  public String makeTopicName() {
    // return "test_topic_" + UUID.randomUUID().toString();
    return "test_topic_20a29ef2-da9c-4f55-953c-0af29dbb392d";
  }

  protected Properties consumerProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("bootstrap.servers", "workstation:9092");
    consumerProperties.setProperty("zookeeper.connect", "workstation:2181");
    consumerProperties.setProperty("group.id", "testGroup");
    consumerProperties.setProperty("auto.offset.reset", "largest");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("auto.commit.interval.ms", "1000");
    consumerProperties.setProperty("session.timeout.ms", "1000");
    return consumerProperties;
  }

  protected Properties producerProperties() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty("bootstrap.servers", "workstation:9092");
    producerProperties.setProperty("key.serializer", "reactor.kafka.support.KeySerializer");
    producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return producerProperties;
  }

}
