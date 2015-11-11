package reactor.pipe.flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.MatchedPipe;
import reactor.pipe.key.Key;
import reactor.pipe.registry.KeyMissMatcher;

public class Flow {

  public Flow() {

  }

  public <K extends Key, V> SubFlow<K, V> start(String name, Subscriber<Tuple2<K, V>> subscriber) {
    return null;
  }

  public static class SubFlow<K extends Key, FROM> {
    public <TO> FlowEnd<K, TO> from(KeyMissMatcher<K> keyMatcher,
                                    MatchedPipe<FROM, TO> pipeline) {
      return null;
    }

    public void to(KeyMissMatcher<K> keyMatcher,
                   Publisher<FROM> publisher) {
    }

  }

  public static class FlowEnd<K, TO> {
    public <K1 extends Key, TO> SubFlow to(Function<K, K1> keyTransposition) {
      return null;
    }
  }



}
