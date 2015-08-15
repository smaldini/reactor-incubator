package reactor.pipe;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.concurrent.LazyVar;
import reactor.pipe.key.Key;
import reactor.pipe.registry.*;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;


public class Firehose<K extends Key> {

  private final static int DEFAULT_RING_BUFFER_SIZE = 2048;

  private final DefaultingRegistry<K>         consumerRegistry;
  private final Consumer<Throwable>           errorHandler;
  private final LazyVar<HashWheelTimer>       timer;
  private final Processor<Runnable, Runnable> processor;

  public Firehose() {
    this(new ConcurrentRegistry<K>(),
         RingBufferProcessor.<Runnable>create(Executors.newFixedThreadPool(2), DEFAULT_RING_BUFFER_SIZE),
         new Consumer<Throwable>() {
           @Override
           public void accept(Throwable throwable) {
             System.out.println(throwable.getMessage());
             throwable.printStackTrace();
           }
         });
  }

  public Firehose(Consumer<Throwable> errorHandler) {
    this(new ConcurrentRegistry<K>(),
         RingBufferProcessor.<Runnable>create(Executors.newFixedThreadPool(2), DEFAULT_RING_BUFFER_SIZE),
         errorHandler);
  }

  public Firehose(DefaultingRegistry<K> registry,
                  Processor<Runnable, Runnable> processor,
                  Consumer<Throwable> dispatchErrorHandler) {
    this.consumerRegistry = registry;
    this.errorHandler = dispatchErrorHandler;
    this.processor = processor;
    this.processor.subscribe(new Subscriber<Runnable>() {

      private volatile Subscription sub;

      @Override
      public void onSubscribe(Subscription subscription) {
        this.sub = subscription;
        subscription.request(1);
      }

      @Override
      public void onNext(Runnable runnable) {
        runnable.run();
        sub.request(1);
      }

      @Override
      public void onError(Throwable throwable) {
        errorHandler.accept(throwable);
      }

      @Override
      public void onComplete() {
        this.sub.cancel();
      }
    });


    this.timer = new LazyVar<>(new Supplier<HashWheelTimer>() {
      @Override
      public HashWheelTimer get() {
        return new HashWheelTimer(10); // TODO: configurable hash wheel size!
      }
    });
  }

  public Firehose<K> fork(ExecutorService executorService,
                          int ringBufferSize) {
    return new Firehose<K>(this.consumerRegistry,
                           RingBufferProcessor.<Runnable>create(executorService, ringBufferSize),
                           this.errorHandler);
  }

  public <V> Firehose notify(final K key, final V ev) {
    Assert.notNull(key, "Key cannot be null.");
    Assert.notNull(ev, "Event cannot be null.");

    processor.onNext(new Runnable() {
      @Override
      public void run() {
        for (Registration<K> reg : consumerRegistry.select(key)) {
          reg.getObject().accept(key, ev);
        }
      }
    });

    return this;
  }

  public <V> Firehose on(final K key, final KeyedConsumer<K, V> consumer) {
    consumerRegistry.register(key, consumer);
    return this;
  }

  public <V> Firehose on(final K key, final SimpleConsumer<V> consumer) {
    consumerRegistry.register(key, new KeyedConsumer<K, V>() {
      @Override
      public void accept(final K key, final V value) {
        consumer.accept(value);
      }
    });
    return this;
  }

  public Firehose miss(final KeyMissMatcher<K> matcher,
                       Function<K, Map<K, KeyedConsumer>> supplier) {
    consumerRegistry.addKeyMissMatcher(matcher, supplier);
    return this;
  }

  public boolean unregister(K key) {
    return consumerRegistry.unregister(key);
  }

  public boolean unregister(Predicate<K> pred) {
    return consumerRegistry.unregister(pred);
  }

  public <V> Registry<K> getConsumerRegistry() {
    return this.consumerRegistry;
  }

  public HashWheelTimer getTimer() {
    return this.timer.get();
  }

  public void shutdown() {
    processor.onComplete();

  }

  public <V> Subscriber<Tuple2<K, V>> makeSubscriber() {
    Firehose<K> ref = this;

    return new Subscriber<Tuple2<K, V>>() {
      private volatile Subscription subscription;

      @Override
      public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1L);
      }

      @Override
      public void onNext(Tuple2<K, V> tuple) {
        ref.notify(tuple.getT1(), tuple.getT2());
        subscription.request(1L);
      }

      @Override
      public void onError(Throwable throwable) {
        ref.errorHandler.accept(throwable);
      }

      @Override
      public void onComplete() {
        subscription.cancel();
      }
    };
  }

  public <V> Publisher<Tuple2<K, V>> makePublisher(K subscriptionKey) {
    Firehose<K> ref = this;


    return new Publisher<Tuple2<K, V>>() {
      @Override
      public void subscribe(Subscriber<? super Tuple2<K, V>> subscriber) {

        AtomicLong requested = new AtomicLong(0);

        Subscription subscription = new Subscription() {
          @Override
          public void request(long l) {
            if (l < 1) {
              throw new RuntimeException("Can't request a non-positive number");
            }

            requested.accumulateAndGet(l, new LongBinaryOperator() {
              @Override
              public long applyAsLong(long left, long right) {
                long sum = left + right;
                if (sum >= Long.MAX_VALUE) {
                  return Long.MAX_VALUE; // Effectively unbounded
                } else {
                  return sum;
                }
              }
            });
          }

          @Override
          public void cancel() {
            ref.unregister(subscriptionKey);
          }
        };

        subscriber.onSubscribe(subscription);

        ref.on(subscriptionKey, new SimpleConsumer<V>() {
          @Override
          public void accept(V value) {
            long r = requested.accumulateAndGet(-1, new LongBinaryOperator() {
              @Override
              public long applyAsLong(long old, long diff) {
                long sum = old + diff;

                if (sum >= Long.MAX_VALUE) {
                  return Long.MAX_VALUE; // Effectively unbounded
                } else if (sum <= 0){
                  return 0;
                } else {
                  return sum;
                }
              }
            });
            if (r >= 0) {
              subscriber.onNext(Tuple.of(subscriptionKey, value));
            }
          }
        });
      }
    };
  }
}