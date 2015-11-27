package reactor.pipe.flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.IPipe;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.selector.Selector;
import reactor.fn.BiFunction;

/**
 * Flow is a higher-level abstraction for composing streams.
 *
 * Flow can be local or distributed, every Flow can have
 * multiple upstreams, multiple processing parts each of which
 * may have one or multiple downstreams.
 */
public interface FlowBuilder {

  Flow flow(String name);

  void start();

  interface Flow {
    <K, V, K1 extends Key> Flow upstream(BiFunction<K, V, K1> keyTransposition,
                                         Publisher<Tuple2<K, V>> subscriber);

    public <TO> Downstream<Key, TO> subscribe(Selector<Key> keyMatcher,
                                              IPipe<?, TO> matchedPipe);

    public <K extends Key, TO> Downstream<K, TO> subscribe(Key key,
                                                           IPipe<?, TO> anonymousPipe);

//    <TO> Flow<K, TO> subscribe(KeyMissMatcher<K> keyMatcher,
//                               IPipe.PipeEnd<V, TO> matchedPipe);

  }

  public interface Downstream<K extends Key, V> {
    void downstream(Consumer<V> consumer);
    void downstream(KeyedConsumer<K, V> consumer);

    <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition);

    <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition,
                                     Subscriber<Tuple2<K1, V>> publisher);

  }


}
