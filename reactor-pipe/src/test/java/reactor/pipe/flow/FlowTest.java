package reactor.pipe.flow;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.MatchedPipe;
import reactor.pipe.key.Key;

public class FlowTest {

  @Test
  public void flowTest() {
    Flow flow = new Flow();
    Subscriber<Tuple2<TheKey, Integer>> kafkaSubscriber = new Subscriber<Tuple2<TheKey, Integer>>() {

      @Override
      public void onSubscribe(Subscription s) {

      }

      @Override
      public void onNext(Tuple2<TheKey, Integer> theKeyIntegerTuple2) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onComplete() {

      }
    };

    flow.start("first-stream", kafkaSubscriber)
        .from((k) -> true,
              MatchedPipe.<Integer>build()
                .map(Object::toString))
        .to(new Function<TheKey, Key>() {
          @Override
          public Key apply(TheKey theKey) {
            return null;
          }
        });


  }

  public class TheKey extends Key {

    public TheKey(Object[] parts) {
      super(parts);
    }
  }

}