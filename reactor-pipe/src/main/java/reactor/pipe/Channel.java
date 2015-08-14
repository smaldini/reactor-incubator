package reactor.pipe;

import org.pcollections.PVector;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.concurrent.Atom;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

public class Channel<T> implements Subscriber<T> {

  private final AnonymousPipe<T> stream;
  private final Atom<PVector<T>> state;
  private final AtomicBoolean    isDrained;

  Channel(AnonymousPipe<T> stream,
          Atom<PVector<T>> state) {
    this.stream = stream;
    this.state = state;
    this.isDrained = new AtomicBoolean(false);
    stream.consume(e -> {
      if (!isDrained.get()) {
        state.update(old -> old.plus(e));
      }
    });
  }

  public T get() {
    if (isDrained.get()) {
      throw new RuntimeException("Channel is already being drained by the stream.");
    }

    return state.updateAndReturnOther(new Function<PVector<T>, Tuple2<PVector<T>, T>>() {
      @Override
      public Tuple2<PVector<T>, T> apply(PVector<T> buffer) {
        if (buffer.size() == 0) {
          return Tuple.of(buffer,
                          null);
        }

        T t = buffer.get(0);
        if (t == null) {
          return null;
        } else {
          return Tuple.of(buffer.subList(1, buffer.size()),
                          t);
        }
      }
    });
  }

  public T get(long time, TimeUnit timeUnit) {
    if (isDrained.get()) {
      throw new RuntimeException("Channel is already being drained by the stream.");
    }

    long start = System.currentTimeMillis();
    T value = state.updateAndReturnOther(new Predicate<PVector<T>>() {
                                           @Override
                                           public boolean test(PVector<T> ts) {
                                             return !ts.isEmpty() ||
                                                    (System.currentTimeMillis() - start > TimeUnit.MILLISECONDS.convert(
                                                      time,
                                                      timeUnit));
                                           }
                                         },
                                         new Function<PVector<T>, Tuple2<PVector<T>, T>>() {
                                           @Override
                                           public Tuple2<PVector<T>, T> apply(PVector<T> buffer) {
                                             if (buffer.size() == 0) {
                                               return Tuple.of(buffer,
                                                               null);
                                             }

                                             T t = buffer.get(0);
                                             if (t == null) {
                                               return null;
                                             } else {
                                               return Tuple.of(buffer.subList(1, buffer.size()),
                                                               t);
                                             }
                                           }
                                         });
    if (value == null) {
      throw new RuntimeException("Channel is empty");
    } else {
      return value;
    }
  }

  public AnonymousPipe<T> stream() {
    this.isDrained.set(true);
    return this.stream;
  }

  public Subscriber<T> subscriber() {
    return this;
  }

  public void dispose() {
    stream.unregister();
  }


  /**
   * Subscriber API
   */

  private volatile Subscription sub;

  @Override
  public void onSubscribe(Subscription subscription) {
    this.sub = subscription;
    subscription.request(1);
  }

  @Override
  public void onNext(T item) {
    stream.notify(item);
    this.sub.request(1);
  }

  public void tell(T item) {
    stream.notify(item);
  }

  @Override
  public void onError(Throwable throwable) {
    // NO OP
  }

  @Override
  public void onComplete() {
    // NO OP
  }
}
