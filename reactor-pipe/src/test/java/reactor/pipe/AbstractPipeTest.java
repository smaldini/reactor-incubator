package reactor.pipe;

import org.junit.Test;
import org.pcollections.TreePVector;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.fn.Consumer;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public abstract class AbstractPipeTest extends AbstractFirehoseTest {

  protected abstract <T, O> void subscribe(IPipe.PipeEnd<T, O> pipe);

  protected abstract <T, O> void subscribeAndDispatch(IPipe.PipeEnd<T, O> pipe, List<T> value);

  protected abstract <T, O> void subscribeAndDispatch(IPipe.PipeEnd<T, O> pipe, T value);

  @Test
  public void mapTest() throws InterruptedException {
    AVar<String> res = new AVar<>();

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .map(i -> i + 1)
        .map(i -> i * 2)
        .map(Object::toString)
        .consume(res::set),
      1);

    assertThat(res.get(1, TimeUnit.SECONDS), is("4"));
  }

  @Test
  public void consumeWithSupplierTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>();

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .map(i -> i + 1)
        .map(i -> i * 2)
        .consume(() -> (k, v) -> res.set(v)),
      1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }

  @Test
  public void statefulMapTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);

    subscribeAndDispatch(
      Pipe.<Integer>build().map((i) -> i + 1)
                           .map((Atom<Integer> state, Integer i) -> {
                                  return state.update(old -> old + i);
                                },
                                0)
                           .consume(res::set),
      Arrays.asList(1, 2, 3));

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
  }

  @Test
  public void scanTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);

    subscribeAndDispatch(
      Pipe.<Integer>build().scan((acc, i) -> acc + i,
                                 0)
                           .consume(res::set),
      Arrays.asList(1, 2, 3));

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(6));
  }

  @Test
  public void debounceTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(1);

    long start = System.currentTimeMillis();
    AtomicLong end = new AtomicLong();
    subscribeAndDispatch(
      Pipe.<Integer>build().debounce(1, TimeUnit.SECONDS)
                           .consume((v) -> {
                             res.set(v);
                             end.set(System.currentTimeMillis());
                           }),
      Arrays.asList(1, 2));

    Thread.sleep(500);

    firehose.notify(Key.wrap("source", "first"),
                    3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(3));

    assertTrue(end.get() - start > 1000 &&
               end.get() - start < 1500);
  }

  @Test
  public void throttleTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(1);

    long start = System.currentTimeMillis();
    AtomicLong end = new AtomicLong();
    subscribeAndDispatch(
      Pipe.<Integer>build().throttle(1, TimeUnit.SECONDS)
                           .consume((v) -> {
                             res.set(v);
                             end.set(System.currentTimeMillis());
                           }),
      Arrays.asList(1, 2));

    Thread.sleep(500);

    firehose.notify(Key.wrap("source", "first"),
                    3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(3));

    assertTrue(end.get() - start > 1500 &&
               end.get() - start < 2000);
  }

  @Test
  public void testFilter() throws InterruptedException {
    AVar<Integer> res = new AVar<>();

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .map(i -> i + 1)
        .filter(i -> i % 2 != 0)
        .map(i -> i * 2)
        .consume(res::set),
      Arrays.asList(1, 2));

    assertThat(res.get(1, TimeUnit.SECONDS), is(6));
  }

  @Test
  public void testPartition() throws InterruptedException {
    AVar<List<Integer>> res = new AVar<>();

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .partition((i) -> {
          return i.size() == 5;
        })
        .consume(res::set),
      Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(1, 2, 3, 4, 5))));
  }

  @Test
  public void testSlide() throws InterruptedException {
    AVar<List<Integer>> res = new AVar<>(6);

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .slide(i -> i.subList(i.size() > 5 ? i.size() - 5 : 0,
                              i.size()))
        .consume(res::set),
      Arrays.asList(1, 2, 3, 4, 5, 6));

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2, 3, 4, 5, 6))));
  }

  @Test
  public void testConsume() throws InterruptedException {
    AVar<Integer> resValue = new AVar<>();
    AVar<Key> resKey = new AVar<>();

    subscribeAndDispatch(
      Pipe.<Integer>build()
        .map((i) -> i + 1)
        .map(i -> i * 2)
        .consume((k, v) -> {
          resKey.set(k);
          resValue.set(v);
        }),
      1);

    assertThat(resKey.get(1, TimeUnit.SECONDS).getPart(0), is("source"));
    assertThat(resValue.get(1, TimeUnit.SECONDS), is(4));
  }


  @Test
  public void testSmoke() throws InterruptedException { // Tests backpressure and in-thread dispatches
    Firehose<Key> concurrentFirehose = new Firehose<>(new ConcurrentRegistry<>(),
                                                      RingBufferWorkProcessor.create(Executors.newFixedThreadPool(4),
                                                                                     256),
                                                      4,
                                                      new Consumer<Throwable>() {
                                                        @Override
                                                        public void accept(Throwable throwable) {
                                                          System.out.printf("Exception caught while dispatching: %s\n",
                                                                            throwable.getMessage());
                                                          throwable.printStackTrace();
                                                        }
                                                      });

    int iterations = 2000;
    CountDownLatch latch = new CountDownLatch(iterations);

    subscribe(
      Pipe.<Integer>build()
        .map((i) -> i + 1)
        .map(i -> {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          return i * 2;
        })
        .consume((i_) -> latch.countDown()));

    for (int i = 0; i < iterations; i++) {
      firehose.notify(Key.wrap("source", "first"), i);
      if (i % 500 == 0) {
        System.out.println("Processed " + i + " keys");
      }
    }

    latch.await(5, TimeUnit.MINUTES);
    assertThat(latch.getCount(), is(0L));

    concurrentFirehose.shutdown();
  }
}
