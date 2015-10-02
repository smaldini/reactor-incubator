package reactor.pipe;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.fn.Consumer;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.key.Key;
import org.junit.Test;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.registry.ConcurrentRegistry;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FirehoseTest extends AbstractFirehoseTest {

  @Test
  public void simpleOnTest() throws InterruptedException {

    AVar<Integer> val = new AVar<>();
    AVar<Integer> val2 = new AVar<>();

    firehose.on(Key.wrap("key1"), val::set);
    firehose.on(Key.wrap("key2"), val2::set);

    firehose.notify(Key.wrap("key1"), 1);
    firehose.notify(Key.wrap("key2"), 2);

    assertThat(val.get(10, TimeUnit.SECONDS), is(1));
    assertThat(val2.get(10, TimeUnit.SECONDS), is(2));
  }

  @Test
  public void doubleSubscriptionTest() throws InterruptedException {
    AVar<Integer> val = new AVar<>();
    AVar<Integer> val2 = new AVar<>();

    firehose.on(Key.wrap("key1"), (key, value) -> {
      return;
    });
    firehose.on(Key.wrap("key1"), val::set);

    firehose.on(Key.wrap("key2"), val2::set);
    firehose.on(Key.wrap("key2"), (key, value) -> {
      return;
    });

    firehose.notify(Key.wrap("key1"), 1);
    firehose.notify(Key.wrap("key2"), 2);

    assertThat(val.get(10, TimeUnit.SECONDS), is(1));
    assertThat(val2.get(10, TimeUnit.SECONDS), is(2));
  }

  @Test
  public void simpleOn2Test() throws InterruptedException {
    AVar<Tuple2> val = new AVar<>();
    firehose.on(Key.wrap("key1"), (key, value) -> {
      val.set(Tuple.of(key, value));
    });
    firehose.notify(Key.wrap("key1"), 1);

    assertThat(val.get(10, TimeUnit.SECONDS),
               is(Tuple.of(Key.wrap("key1"), 1)));
  }

  @Test
  public void keyMissTest() throws InterruptedException {
    AVar<Tuple2> val = new AVar<>();

    firehose.miss((k_) -> true,
                  (k) -> {
                    return Collections.singletonMap(k, (key, value) -> {
                      val.set(Tuple.of(key, value));
                    });
                  });

    firehose.notify(Key.wrap("key1"), 1);

    assertThat(val.get(10, TimeUnit.SECONDS), is(Tuple.of(Key.wrap("key1"), 1)));
  }

  @Test
  public void unsubscribeTest() throws InterruptedException {
    Key k = Key.wrap("key1");
    final CountDownLatch latch2 = new CountDownLatch(2);
    final CountDownLatch latch1 = new CountDownLatch(1);

    firehose.on(k, (i) -> {
      latch2.countDown();
      latch1.countDown();
    });

    firehose.notify(k, 1);
    latch1.await(10, TimeUnit.SECONDS);
    firehose.unregister(k);
    firehose.notify(k, 1);

    latch2.await(1, TimeUnit.SECONDS);
    assertThat(latch2.getCount(), is(1L));
  }

  @Test
  public void unsubscribeAllTest() throws InterruptedException {
    Key k1 = Key.wrap("key1");
    Key k2 = Key.wrap("key2");
    Key k3 = Key.wrap("_key3");

    final CountDownLatch latch = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(1);

    firehose.on(k1, (i) -> {
      latch.countDown();
    });

    firehose.on(k2, (i) -> {
      latch.countDown();
    });

    firehose.on(k3, (i) -> {
      latch2.countDown();
    });

    firehose.unregister(k -> ((String) k.getPart(0)).startsWith("key"));
    firehose.notify(k1, 1);
    firehose.notify(k2, 1);
    firehose.notify(k3, 1);

    latch2.await(10, TimeUnit.SECONDS);

    assertThat(latch2.getCount(), is(0L));
    assertThat(latch.getCount(), is(2L));
  }

  @Test
  public void errorTest() throws InterruptedException {
    AVar<Throwable> caught = new AVar<>();
    Firehose<Key> asyncFirehose = new Firehose<>(throwable -> caught.set(throwable));
    Key k1 = Key.wrap("key1");

    asyncFirehose.on(k1, (Integer i) -> {
      int j = i / 0;
    });

    asyncFirehose.notify(k1, 1);

    assertTrue(caught.get(1, TimeUnit.MINUTES) instanceof ArithmeticException);

    asyncFirehose.shutdown();
  }

  @Test
  public void subscriberTest() throws InterruptedException {
    Subscriber<Tuple2<Key, Integer>> subscriber = firehose.makeSubscriber();
    AVar<Integer> val = new AVar<>();
    AVar<Integer> val2 = new AVar<>();

    AtomicLong requested = new AtomicLong(0);
    AtomicBoolean cancelCalled = new AtomicBoolean(false);
    Subscription subscription = new Subscription() {
      @Override
      public void request(long l) {
        requested.addAndGet(l);
      }

      @Override
      public void cancel() {
        cancelCalled.set(true);
      }
    };

    subscriber.onSubscribe(subscription);
    assertThat(requested.get(), is(1L));

    firehose.on(Key.wrap("key1"), val::set);
    assertThat(requested.get(), is(1L));
    firehose.on(Key.wrap("key2"), val2::set);
    assertThat(requested.get(), is(1L));

    subscriber.onNext(Tuple.of(Key.wrap("key1"), 1));
    subscriber.onNext(Tuple.of(Key.wrap("key2"), 2));

    assertThat(val.get(10, TimeUnit.SECONDS), is(1));
    assertThat(val2.get(10, TimeUnit.SECONDS), is(2));

    subscriber.onComplete();
    assertThat(cancelCalled.get(), is(true));
  }

  @Test
  public void publisherTest() throws InterruptedException, TimeoutException, BrokenBarrierException {
    Publisher<Tuple2<Key, Integer>> publisher = firehose.makePublisher(Key.wrap("key1"));

    AtomicReference<Tuple2<Key, Integer>> tupleRef = new AtomicReference<>();
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(2);
    CountDownLatch latch3 = new CountDownLatch(3);

    Subscriber<Tuple2<Key, Integer>> subscriber = new Subscriber<Tuple2<Key, Integer>>() {
      private volatile Subscription subscription;
      @Override
      public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
      }

      @Override
      public void onNext(Tuple2<Key, Integer> tuple) {
        tupleRef.set(tuple);
        subscription.request(1);
        try {
          latch1.countDown();
          latch2.countDown();
          latch3.countDown();
        } catch (Exception e) {
          //
        }
      }

      @Override
      public void onError(Throwable throwable) {

      }

      @Override
      public void onComplete() {
        subscription.cancel();
      }
    };

    publisher.subscribe(subscriber);

    firehose.notify(Key.wrap("key1"), 1);

    latch1.await(10, TimeUnit.SECONDS);
    assertThat(tupleRef.get().getT2(), is(1));

    firehose.notify(Key.wrap("key1"), 2);
    latch2.await(10, TimeUnit.SECONDS);
    assertThat(tupleRef.get().getT2(), is(2));

    subscriber.onComplete();
    firehose.notify(Key.wrap("key1"), 3);
    latch2.await(1, TimeUnit.SECONDS);
    assertThat(tupleRef.get().getT2(), is(2));
  }

  @Test
  public void smokeTest() throws InterruptedException {
    Firehose<Key> concurrentFirehose = new Firehose<>(new ConcurrentRegistry<>(),
                                                      RingBufferWorkProcessor.create(Executors.newFixedThreadPool(4),
                                                                                     2048),
                                                      4,
                                                      new Consumer<Throwable>() {
                                                        @Override
                                                        public void accept(Throwable throwable) {
                                                          System.out.printf("Exception caught while dispatching: %s\n",
                                                                            throwable.getMessage());
                                                          throwable.printStackTrace();
                                                        }
                                                      });
    int iterations = 10000;
    CountDownLatch latch = new CountDownLatch(iterations);
    CountDownLatch latch2 = new CountDownLatch(iterations);
    CountDownLatch latch3 = new CountDownLatch(iterations);
    concurrentFirehose.on(Key.wrap("key1"), (i_) -> latch.countDown());
    concurrentFirehose.on(Key.wrap("key2"), (i_) -> latch2.countDown());
    concurrentFirehose.on(Key.wrap("key3"), (i_) -> latch3.countDown());

    for (int i = 0; i < iterations; i++) {
      concurrentFirehose.notify(Key.wrap("key1"), i);
      concurrentFirehose.notify(Key.wrap("key2"), i);
      concurrentFirehose.notify(Key.wrap("key3"), i);
      if (i % 1000 == 0) {
        System.out.println("Dispatched " + i + " keys");
      }
    }

    latch.await(5, TimeUnit.MINUTES);
    latch2.await(5, TimeUnit.MINUTES);
    latch3.await(5, TimeUnit.MINUTES);
    
    assertThat(latch.getCount(), is(0L));
    assertThat(latch2.getCount(), is(0L));
    assertThat(latch3.getCount(), is(0L));

    concurrentFirehose.shutdown();
  }


}
