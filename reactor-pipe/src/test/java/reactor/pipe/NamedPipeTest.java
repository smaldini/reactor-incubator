package reactor.pipe;

import org.junit.Test;
import org.pcollections.TreePVector;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Predicate;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.state.StateProvider;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class NamedPipeTest extends AbstractFirehoseTest {

  @Test
  public void testMap() throws InterruptedException {
    AVar<Integer> res = new AVar<>();
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.map(Key.wrap("key1"), Key.wrap("key2"), (i) -> i + 1);
    intPipe.consume(Key.wrap("key2"), res::set);

    intPipe.notify(Key.wrap("key1"), 1);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(2));
  }

  @Test
  public void debounceTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>();
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);


    intPipe.map(Key.wrap("key1"), Key.wrap("key2"), (i) -> i + 1);
    intPipe.debounce(Key.wrap("key2"), Key.wrap("key3"), 100, TimeUnit.MILLISECONDS);

    {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicInteger counter = new AtomicInteger(0);
      intPipe.consume(Key.wrap("key3"), (Integer i) -> {
        res.set(i);
        counter.incrementAndGet();
        latch.countDown();
      });

      intPipe.notify(Key.wrap("key1"), 1);
      intPipe.notify(Key.wrap("key1"), 2);
      intPipe.notify(Key.wrap("key1"), 3);
      latch.await(1, TimeUnit.SECONDS);
      assertThat(counter.get(), is(1));
    }
    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(4));
  }

  @Test
  public void streamStateSupplierTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.map(Key.wrap("key1"), Key.wrap("key2"), (i) -> i + 1);
    intPipe.map(Key.wrap("key2"), Key.wrap("key3"), (Atom<Integer> state) -> {
                  return (i) -> {
                    return state.update(old -> old + i);
                  };
                },
                0);
    intPipe.consume(Key.wrap("key3"), res::set);

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
  }

  @Test
  public void streamStateFnTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.map(Key.wrap("key1"), Key.wrap("key2"), (i) -> i + 1);
    intPipe.map(Key.wrap("key2"), Key.wrap("key3"), (Atom<Integer> state, Integer i) -> {
                  return state.update(old -> old + i);
                },
                0);
    intPipe.consume(Key.wrap("key3"), res::set);

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
  }

  @Test
  public void streamFilterTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(2);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.filter(Key.wrap("key1"), Key.wrap("key2"), (i) -> i % 2 == 0);
    intPipe.consume(Key.wrap("key2"), res::set);

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);
    intPipe.notify(Key.wrap("key1"), 4);
    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(4));
  }

  @Test
  public void divideTest() throws InterruptedException {
    CountDownLatch evenLatch = new CountDownLatch(5);
    CountDownLatch oddLatch = new CountDownLatch(5);
    List<Integer> even = new CopyOnWriteArrayList<>();
    List<Integer> odd = new CopyOnWriteArrayList<>();

    Key evenKey = Key.wrap("even");
    Key oddKey = Key.wrap("odd");
    Key numbersKey = Key.wrap("numbers");

    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);
    intPipe.divide(numbersKey,
                   (k, v) -> {
                     return v % 2 == 0 ? evenKey : oddKey;
                   });

    intPipe.consume(evenKey, (Integer i) -> {
      even.add(i);
      evenLatch.countDown();
    });

    intPipe.consume(oddKey, (Integer i) -> {
      odd.add(i);
      oddLatch.countDown();
    });

    for (int i = 0; i < 10; i++) {
      intPipe.notify(numbersKey, i);
    }

    evenLatch.await(LATCH_TIMEOUT, LATCH_TIME_UNIT);
    assertThat(even, is(Arrays.asList(0, 2, 4, 6, 8)));
    oddLatch.await(LATCH_TIMEOUT, LATCH_TIME_UNIT);
    assertThat(odd, is(Arrays.asList(1, 3, 5, 7, 9)));
  }

  @Test
  public void partitionTest() throws InterruptedException {
    AVar<List<Integer>> res = new AVar<>();
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.partition(Key.wrap("key1"), Key.wrap("key2"), (i) -> {
      return i.size() == 5;
    });
    intPipe.consume(Key.wrap("key2"), (List<Integer> a) -> {
      res.set(a);
    });

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);
    intPipe.notify(Key.wrap("key1"), 4);
    intPipe.notify(Key.wrap("key1"), 5);
    intPipe.notify(Key.wrap("key1"), 6);
    intPipe.notify(Key.wrap("key1"), 7);

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(1, 2, 3, 4, 5))));
  }

  @Test
  public void slideTest() throws InterruptedException {
    AVar<List<Integer>> res = new AVar<>(6);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.slide(Key.wrap("key1"), Key.wrap("key2"), (i) -> {
      return i.subList(i.size() > 5 ? i.size() - 5 : 0,
                       i.size());
    });
    intPipe.consume(Key.wrap("key2"), (List<Integer> a) -> {
      res.set(a);
    });

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);
    intPipe.notify(Key.wrap("key1"), 4);
    intPipe.notify(Key.wrap("key1"), 5);
    intPipe.notify(Key.wrap("key1"), 6);
    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2, 3, 4, 5, 6))));
  }

  @Test
  public void testUnregister() throws InterruptedException {
    Key k1 = Key.wrap("key1");
    Key k2 = Key.wrap("key2");

    CountDownLatch latch = new CountDownLatch(1);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.map(k1, k2, (i) -> i + 1);
    intPipe.consume(k2, (i) -> latch.countDown());

    intPipe.unregister(k2);
    intPipe.notify(k1, 1);

    assertThat(latch.getCount(), is(1L));
  }

  @Test
  public void testUnregisterAll() throws InterruptedException {
    Key k1 = Key.wrap("key1");
    Key k2 = Key.wrap("key2");

    CountDownLatch latch = new CountDownLatch(2);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.map(k1, k2, (i) -> {
      latch.countDown();
      return i + 1;
    });

    intPipe.consume(k2, (i) -> latch.countDown());

    intPipe.unregister((Predicate<Key>) key -> true);
    intPipe.notify(k1, 1);

    assertThat(latch.getCount(), is(2L));
  }

  @Test
  public void testDispatchers() throws InterruptedException {
    Key k1 = Key.wrap("key1");
    Key k2 = Key.wrap("key2");

    CountDownLatch latch = new CountDownLatch(1);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.fork(Executors.newFixedThreadPool(2),
                 2,
                 2048)
           .map(k1, k2, (i) -> {
             try {
               Thread.sleep(100);
             } catch (InterruptedException e) {
               e.printStackTrace();
             }
             return i;
           });

    intPipe.consume(k2, (i) -> latch.countDown());

    intPipe.unregister((Predicate<Key>) key -> true);
    intPipe.notify(k1, 1);

    assertThat(latch.getCount(), is(1L));
  }

  @Test
  public void testExceptionInLoading() throws InterruptedException {
    Key k1 = Key.wrap("key1");

    CountDownLatch latch = new CountDownLatch(2);
    Firehose<Key> firehose = new Firehose<>(new Consumer<Throwable>() {
      @Override
      public void accept(Throwable throwable) {
        latch.countDown();
      }
    });
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose,
                                                 new StateProvider<Key>() {
                                                   @Override
                                                   public <T> Atom<T> makeAtom(Key src, T init) {
                                                     throw new RuntimeException();
                                                   }
                                                 });

    intPipe.matched((k) -> true)
           .map(new BiFunction<Atom<Integer>, Integer, Integer>() {
                  @Override
                  public Integer apply(Atom<Integer> st, Integer i) {
                    return st.update((acc) -> acc + i);
                  }
                },
                0)
           .consume(System.out::println);

    firehose.notify(k1, 1);
    firehose.notify(k1, 1);

    latch.await(10, TimeUnit.SECONDS);
    assertThat(latch.getCount(), is(0L));
  }


}
