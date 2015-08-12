package reactor.kafka;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class KafkaSubscriber<T> implements Subscriber<T> {

  @Override
  public void onSubscribe(Subscription subscription) {

  }

  @Override
  public void onNext(T t) {

  }

  @Override
  public void onError(Throwable throwable) {

  }

  @Override
  public void onComplete() {

  }
}
