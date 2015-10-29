package reactor.pipe;

import org.junit.Test;
import org.pcollections.TreePVector;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MatchedPipeTest extends AbstractFirehoseTest {

  @Test
  public void mapTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<Integer> res = new AVar<>();

    pipe.matched(key -> key.getPart(0).equals("source"))
        .map(i -> i + 1)
        .map(i -> i * 2)
        .consume(res::set);

    pipe.notify(Key.wrap("source", "first"), 1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }

  @Test
  public void consumeWithSupplierTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<Integer> res = new AVar<>();

    pipe.matched(key -> key.getPart(0).equals("source"))
        .map(i -> i + 1)
        .map(i -> i * 2)
        .consume(() -> (k, v) -> res.set(v));

    pipe.notify(Key.wrap("source", "first"), 1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }

  @Test
  public void statefulMapTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);
    NamedPipe<Integer> intPipe = new NamedPipe<>(firehose);

    intPipe.matched((key) -> key.getPart(0).equals("source"))
           .map((i) -> i + 1)
           .map((Atom<Integer> state, Integer i) -> {
                  return state.update(old -> old + i);
                },
                0)
           .consume(res::set);

    intPipe.notify(Key.wrap("source", "1"), 1);
    intPipe.notify(Key.wrap("source", "1"), 2);
    intPipe.notify(Key.wrap("source", "1"), 3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
  }

  @Test
  public void testFilter() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<Integer> res = new AVar<>();

    pipe.matched(key -> key.getPart(0).equals("source"))
        .map(i -> i + 1)
        .filter(i -> i % 2 != 0)
        .map(i -> i * 2)

        .consume(res::set);


    pipe.notify(Key.wrap("source"), 1);
    pipe.notify(Key.wrap("source"), 2);

    assertThat(res.get(1, TimeUnit.SECONDS), is(6));
  }

  @Test
  public void testPartition() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<List<Integer>> res = new AVar<>();

    pipe.matched(key -> key.getPart(0).equals("source"))
        .partition((i) -> {
          return i.size() == 5;
        })
        .consume(res::set);

    pipe.notify(Key.wrap("source"), 1);
    pipe.notify(Key.wrap("source"), 2);
    pipe.notify(Key.wrap("source"), 3);
    pipe.notify(Key.wrap("source"), 4);
    pipe.notify(Key.wrap("source"), 5);
    pipe.notify(Key.wrap("source"), 6);
    pipe.notify(Key.wrap("source"), 7);

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(1, 2, 3, 4, 5))));
  }

  @Test
  public void testSlide() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<List<Integer>> res = new AVar<>(6);

    pipe.matched((key) -> key.getPart(0).equals("source"))
        .slide(i -> {
          return i.subList(i.size() > 5 ? i.size() - 5 : 0,
                           i.size());
        })
        .consume(res::set);

    pipe.notify(Key.wrap("source"), 1);
    pipe.notify(Key.wrap("source"), 2);
    pipe.notify(Key.wrap("source"), 3);
    pipe.notify(Key.wrap("source"), 4);
    pipe.notify(Key.wrap("source"), 5);
    pipe.notify(Key.wrap("source"), 6);

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2, 3, 4, 5, 6))));
  }

  @Test
  public void testMatching() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    AVar<Integer> res1 = new AVar<>(1);
    AVar<Integer> res2 = new AVar<>(1);

    for (AVar<Integer> avar : new AVar[]{res1, res2}) {
      pipe.matched(key -> key.getPart(0).equals("source"))
          .consume(avar::set);
    }

    firehose.notify(Key.wrap("source"), 100);

    assertThat(res1.get(1, TimeUnit.SECONDS), is(100));
    assertThat(res2.get(1, TimeUnit.SECONDS), is(100));
  }


}
