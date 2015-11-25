package reactor.pipe.flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.Firehose;
import reactor.pipe.IPipe;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.KeyMissMatcher;

import java.util.function.BiFunction;

public class LocalFlowBuilder implements FlowBuilder{

  private final Firehose firehose;

  public LocalFlowBuilder() {
    this(new Firehose());
  }

  public LocalFlowBuilder(Firehose firehose) {
    this.firehose = firehose;
  }

  @Override
  public <K extends Key, V> Flow<K, V> flow(String name) {
    return new LocalFlow<>(firehose);
  }

  public class LocalFlow<K extends Key, V> implements Flow<K, V> {

    private Firehose firehose;

    public LocalFlow(Firehose firehose) {
      this.firehose = firehose;
    }

    @Override
    public <K1 extends Key> Flow<K1, V> upstream(Publisher<Tuple2<K, V>> publisher,
                                                 BiFunction<K, V, K1> keyTransposition) {
      publisher.subscribe(firehose.makeSubscriber(keyTransposition));
      return new LocalFlow<>(firehose);
    }

    @Override
    public <TO> Downstream<K, TO> subscribe(KeyMissMatcher<K> keyMatcher, IPipe<V, TO> matchedPipe) {
      return null;
    }
  }

  public class LocalDownstream<K extends Key, V> implements Downstream<K, V> {

    private final Firehose firehose;
    private final IPipe<K, V> upstreamPipe;
    private final KeyMissMatcher<K> matcher;

    public LocalDownstream(Firehose firehose,
                           IPipe<K, V> upstreamPipe,
                           KeyMissMatcher<K> matcher) {
      this.firehose = firehose;
      this.upstreamPipe = upstreamPipe;
      this.matcher = matcher;
    }
    @Override
    public <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition) {
      upstreamPipe.consume(new KeyedConsumer<K, V>() {
        @Override
        public void accept(K key, V value) {
          firehose.notify(keyTransposition.apply(key, value), value);
        }
      }).subscribe(matcher, firehose);
    }

    @Override
    public <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition,
                                            Subscriber<Tuple2<K1, V>> publisher) {
      upstreamPipe.consume(new KeyedConsumer<K, V>() {
        @Override
        public void accept(K key, V value) {
          publisher.onNext(Tuple.of(keyTransposition.apply(key,value), value));
        }
      }).subscribe(matcher, firehose);
    }
  }
}
