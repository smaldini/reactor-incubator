package reactor.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple2;

import java.util.Properties;

public class KafkaSubscriber<K, V> implements Subscriber<Tuple2<K, V>> {

  private final    KafkaProducer<K, V>  kafkaProducer;
  private final    String               topic;
  private final    Function<K, Integer> partitioner;
  private final    Consumer<Throwable>  errorConsumer;
  private volatile Subscription         subscription;

  public KafkaSubscriber(Properties producerProperties,
                         String topic,
                         Function<K, Integer> partitioner,
                         Consumer<Throwable> errorConsumer) {
    this.kafkaProducer = new KafkaProducer<>(producerProperties);
    this.topic = topic;
    this.partitioner = partitioner;
    this.errorConsumer = errorConsumer;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Tuple2<K, V> tuple) {
    ProducerRecord<K, V> record = new ProducerRecord<>(topic,
                                                       partitioner.apply(tuple.getT1()),
                                                       tuple.getT1(),
                                                       tuple.getT2());
    kafkaProducer.send(record);
    this.subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onError(Throwable throwable) {
    errorConsumer.accept(throwable);
  }

  @Override
  public void onComplete() {
    this.kafkaProducer.close();
  }
}
