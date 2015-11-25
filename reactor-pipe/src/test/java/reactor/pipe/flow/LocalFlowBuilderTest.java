package reactor.pipe.flow;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.AbstractFirehoseTest;
import reactor.pipe.Pipe;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LocalFlowBuilderTest extends AbstractFirehoseTest {

  @Test
  public void upDownstreamTest() throws InterruptedException {
    AVar<Integer> result = new AVar<>(1);

    FlowBuilder localFlowBuilder = new LocalFlowBuilder(firehose);

    List<Tuple2<String, Integer>> l = Arrays.asList(Tuple.of("source", 1),
                                                    Tuple.of("source", 2));
    Publisher<Tuple2<String, Integer>> p = Publishers.from(l);

    localFlowBuilder.<String, Integer>flow("local flow")
      .upstream((String k, Integer v) -> Key.wrap(k),
                p)
      .subscribe(Key.wrap("source"),
                 Pipe.<Integer>build().map((Integer i) -> i + 1))
      .downstream(result::set);

    localFlowBuilder.start();
    assertThat(result.get(LATCH_TIMEOUT, LATCH_TIME_UNIT),
               is(2));
  }


}