package reactor.pipe.flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.Pipe;
import reactor.pipe.key.Key;
import reactor.pipe.registry.KeyMissMatcher;

import java.util.function.BiFunction;

/**
 * Flow is a higher-level abstraction for composing streams together.
 *
 * Every Flow can have multiple upstreams, multiple processing parts
 * each of which may have one or multiple downstreams.
 */
public interface FlowBuilder {

  <K extends Key, V> Flow<K, V> flow(String name);

  interface Flow<K extends Key, V> {
    <K1 extends Key> Flow<K1, V> upstream(Subscriber<Tuple2<K, V>> subscriber,
                                          BiFunction<K, V, K1> keyTransposition);


    <TO> Downstream<K, TO> subscribe(KeyMissMatcher<K> keyMatcher,
                                     Pipe<V, TO> matchedPipe);

  }

  public interface Downstream<K extends Key, V> {
    <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition);

    <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition,
                                     Publisher<Tuple2<K1, V>> publisher);

  }


}
