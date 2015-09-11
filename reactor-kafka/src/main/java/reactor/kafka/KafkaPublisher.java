package reactor.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reads the items from the kafka queue and puts them into the publisher
 */
public class KafkaPublisher<K, V> implements Publisher<Tuple2<K, V>> {

  private final static int concurrencyLevel = 1;

  private final ExecutorService                                     executor;
  private final Map<Subscription, Subscriber<? super Tuple2<K, V>>> subscribers;

  public KafkaPublisher(Properties consumerProperties,
                        String topic,
                        Decoder<K> keyDecoder,
                        Decoder<V> valueDecoder) {
    this.subscribers = new ConcurrentHashMap<>();
    this.executor = Executors.newFixedThreadPool(concurrencyLevel);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
      new ConsumerConfig(consumerProperties));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, concurrencyLevel);
    Map<String, List<KafkaStream<K, V>>> consumerMap =
      consumer.createMessageStreams(topicCountMap,
                                    keyDecoder,
                                    valueDecoder);
    List<KafkaStream<K, V>> streams = consumerMap.get(topic);

    for (final KafkaStream stream : streams) {
      executor.execute(() -> {
        ConsumerIterator<K, V> it = stream.iterator();
        while (it.hasNext() && !Thread.currentThread().isInterrupted()) {
          MessageAndMetadata<K, V> msg = it.next();
          Tuple2<K, V> tuple = Tuple.of(msg.key(), msg.message());
          for (Map.Entry<Subscription, Subscriber<? super Tuple2<K, V>>> entry : subscribers.entrySet()) {
            entry.getValue().onNext(tuple);
          }

        }
      });
    }

  }

  @Override
  public void subscribe(Subscriber<? super Tuple2<K, V>> subscriber) {
    Subscription subscription = new KafkaSubscription<>(subscriber,
                                                        subscribers::remove);
    subscriber.onSubscribe(subscription);

    this.subscribers.put(subscription,
                         subscriber);
  }

}
