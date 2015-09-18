package reactor.pipe.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class FirehoseSubscriber implements Subscriber<Runnable> {

  private Subscription subscription;

  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1L);
  }

  @Override
  public void onNext(Runnable runnable) {
    runnable.run();
    subscription.request(1L);
  }

  @Override
  public void onError(Throwable throwable) {

  }

  @Override
  public void onComplete() {

  }
}

