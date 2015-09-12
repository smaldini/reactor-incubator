package reactor.pipe;

import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import org.junit.Test;
import org.pcollections.TreePVector;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class AnonymousNamedPipeTest extends AbstractStreamTest {

  @Test
  public void testMap() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<Integer> res = new AVar<>();

    pipe.anonymous(Key.wrap("source"))
          .map((i) -> i + 1)
          .map(i -> i * 2)
          .consume(res::set);


    pipe.notify(Key.wrap("source"), 1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }

  @Test
  public void statefulMapTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>(3);
    NamedPipe<Integer> intPipe = new NamedPipe<>();

    intPipe.anonymous(Key.wrap("key1"))
           .map((i) -> i + 1)
           .map((Atom<Integer> state, Integer i) -> {
                  return state.update(old -> old + i);
                },
                0)
           .consume(res::set);

    intPipe.notify(Key.wrap("key1"), 1);
    intPipe.notify(Key.wrap("key1"), 2);
    intPipe.notify(Key.wrap("key1"), 3);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
  }

  @Test
  public void testFilter() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<Integer> res = new AVar<>();

    pipe.anonymous(Key.wrap("source"))
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
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<List<Integer>> res = new AVar<>();

    pipe.anonymous(Key.wrap("source"))
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
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<List<Integer>> res = new AVar<>(6);

    pipe.anonymous(Key.wrap("source"))
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

    assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2,3,4,5,6))));
  }

  @Test
  public void testNotify() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<Integer> res = new AVar<>();

    AnonymousPipe<Integer> s = pipe.anonymous(Key.wrap("source"));

    s.map((i) -> i + 1)
     .map(i -> i * 2)
     .consume(res::set);

    pipe.notify(Key.wrap("source"), 1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }

  @Test
  public void testUnregister() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    CountDownLatch latch = new CountDownLatch(2);

    AnonymousPipe<Integer> s = pipe.anonymous(Key.wrap("source"));

    s.map((i) -> i + 1)
     .map(i -> i * 2)
     .consume(i -> latch.countDown());

    pipe.notify(Key.wrap("source"), 1);
    s.unregister();
    pipe.notify(Key.wrap("source"), 1);

    assertThat(latch.getCount(), is(1L));
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(0L));
  }

  @Test
  public void testRedirect() throws InterruptedException {
    Key destination = Key.wrap("destination");
    NamedPipe<Integer> pipe = new NamedPipe<>();
    AVar<Integer> res = new AVar<>();

    AnonymousPipe<Integer> s = pipe.anonymous(Key.wrap("source"));

    s.map((i) -> i + 1)
     .map(i -> i * 2)
     .redirect(destination);

    pipe.consume(destination, (Integer i) -> res.set(i));

    pipe.notify(Key.wrap("source"), 1);

    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
  }
}
