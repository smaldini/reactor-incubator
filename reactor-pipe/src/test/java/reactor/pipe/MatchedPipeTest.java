package reactor.pipe;

import org.junit.Test;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.key.Key;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MatchedPipeTest extends AbstractPipeTest {

  @Test
  public void testMatching() throws InterruptedException {
    AVar<Integer> res1 = new AVar<>(1);
    AVar<Integer> res2 = new AVar<>(1);

    for (AVar<Integer> avar : new AVar[]{res1, res2}) {
      Pipe.<Integer>build()
        .consume(avar::set)
        .subscribe(key -> key.getPart(0).equals("source"),
                   firehose);
    }

    firehose.notify(Key.wrap("source"), 100);

    assertThat(res1.get(1, TimeUnit.SECONDS), is(100));
    assertThat(res2.get(1, TimeUnit.SECONDS), is(100));
  }


  @Override
  protected <T, O> void subscribe(IPipe.PipeEnd<T, O> pipe) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);
  }

  @Override
  protected <T, O> void subscribeAndDispatch(IPipe.PipeEnd<T, O> pipe, List<T> values) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);

    for(T value: values) {
      firehose.notify(Key.wrap("source", "first"), value);
    }
  }

  @Override
  protected <T, O> void subscribeAndDispatch(IPipe.PipeEnd<T, O> pipe, T value) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);

    firehose.notify(Key.wrap("source", "first"), 1);
  }


}
