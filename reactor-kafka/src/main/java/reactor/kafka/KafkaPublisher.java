package reactor.kafka;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class KafkaPublisher<T> implements Publisher<T> {

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {

  }
}
