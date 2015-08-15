package reactor.kafka;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

public class KafkaSubscription<T> implements Subscription {

  private final AtomicLong                     requested;
  private final Subscriber<T>                  subscriber;
  private final Consumer<KafkaSubscription<T>> cancel;

  public KafkaSubscription(Subscriber<T> subscriber,
                           Consumer<KafkaSubscription<T>> cancel) {
    this.requested = new AtomicLong(0);
    this.subscriber = subscriber;
    this.cancel = cancel;
  }

  @Override
  public void request(long l) {
    if (l < 1) {
      throw new RuntimeException("Can't request a non-positive number");
    }

    requested.accumulateAndGet(l, new LongBinaryOperator() {
      @Override
      public long applyAsLong(long old, long diff) {
        long sum = old + diff;
        if (sum < 0 || sum == Long.MAX_VALUE) {
          return Long.MAX_VALUE; // Effectively unbounded
        } else {
          return sum;
        }
      }
    });
  }

  public void offer(T t) {
    long r = requested.accumulateAndGet(-1, new LongBinaryOperator() {
      @Override
      public long applyAsLong(long old, long diff) {
        long sum = old + diff;

        if (old == Long.MAX_VALUE || sum <= 0) {
          return Long.MAX_VALUE; // Effectively unbounded
        } else if (old == 1) {
          return 0;
        } else {
          return sum;
        }
      }
    });
    if (r >= 0) {
      subscriber.onNext(t);
    }
  }


  @Override
  public void cancel() {
    cancel.accept(this);
  }
}
