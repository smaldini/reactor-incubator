package reactor.pipe.flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.Firehose;
import reactor.pipe.IPipe;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.Selector;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class LocalFlowBuilder implements FlowBuilder {

  private final Firehose                            firehose;
  private final List<Tuple2<Publisher, BiFunction>> publishers;

  public LocalFlowBuilder() {
    this(new Firehose());
  }

  public LocalFlowBuilder(Firehose firehose) {
    this.firehose = firehose;
    this.publishers = new ArrayList<>();
  }

  @Override
  public Flow flow(String name) {
    return new LocalFlow(firehose);
  }

  @Override
  public void start() {
    for (Tuple2<Publisher, BiFunction> tuple : publishers) {
      tuple.getT1().subscribe(firehose.makeSubscriber(tuple.getT2()));
    }
  }


  public class LocalFlow implements Flow {

    private Firehose firehose;

    public LocalFlow(Firehose firehose) {
      this.firehose = firehose;

    }

    @Override
    public <K, V, K1 extends Key> Flow upstream(BiFunction<K, V, K1> keyTransposition,
                                                Publisher<Tuple2<K, V>> publisher) {
      publishers.add(Tuple.of(publisher, keyTransposition));
      return new LocalFlow(firehose);
    }

    @Override
    public <TO> Downstream<Key, TO> subscribe(Selector<Key> keyMatcher,
                                              IPipe<?, TO> matchedPipe) {
      return new MatchedLocalDownstream<>(firehose, matchedPipe, keyMatcher);
    }

    @Override
    public <K extends Key, TO> Downstream<K, TO> subscribe(Key key,
                                                           IPipe<?, TO> anonymousPipe) {
      return new AnonymousLocalDownstream<>(firehose, anonymousPipe, key);
    }


  }

  private class MatchedLocalDownstream<V> implements Downstream<Key, V> {

    private final Firehose    firehose;
    private final IPipe<?, V> upstreamPipe;
    private final Selector<Key> matcher;

    public MatchedLocalDownstream(Firehose firehose,
                                  IPipe<?, V> upstreamPipe,
                                  Selector<Key> matcher) {
      this.firehose = firehose;
      this.upstreamPipe = upstreamPipe;
      this.matcher = matcher;
    }

    @Override
    public void downstream(Consumer<V> consumer) {
      upstreamPipe.consume(consumer).subscribe(matcher, firehose);
    }

    @Override
    public void downstream(KeyedConsumer<Key, V> consumer) {
      upstreamPipe.consume(consumer).subscribe(matcher, firehose);
    }

    @Override
    public <K1 extends Key> void downstream(BiFunction<Key, V, K1> keyTransposition) {
      upstreamPipe.consume(new KeyedConsumer<Key, V>() {
        @Override
        public void accept(Key key, V value) {
          firehose.notify(keyTransposition.apply(key, value), value);
        }
      }).subscribe(matcher, firehose);
    }

    @Override
    public <K1 extends Key> void downstream(BiFunction<Key, V, K1> keyTransposition,
                                            Subscriber<Tuple2<K1, V>> publisher) {
      upstreamPipe.consume(new KeyedConsumer<Key, V>() {
        @Override
        public void accept(Key key, V value) {
          publisher.onNext(Tuple.of(keyTransposition.apply(key, value), value));
        }
      }).subscribe(matcher, firehose);
    }
  }

  private class AnonymousLocalDownstream<K extends Key, V> implements Downstream<K, V> {

    private final Firehose    firehose;
    private final IPipe<?, V> upstreamPipe;
    private final Key         key;

    public AnonymousLocalDownstream(Firehose firehose,
                                    IPipe<?, V> upstreamPipe,
                                    Key key) {
      this.firehose = firehose;
      this.upstreamPipe = upstreamPipe;
      this.key = key;
    }

    @Override
    public void downstream(Consumer<V> consumer) {
      upstreamPipe.consume(consumer).subscribe(key, firehose);
    }

    @Override
    public void downstream(KeyedConsumer<K, V> consumer) {
      upstreamPipe.consume(consumer).subscribe(key, firehose);
    }

    @Override
    public <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition) {
      upstreamPipe.consume(new KeyedConsumer<K, V>() {
        @Override
        public void accept(K key, V value) {
          firehose.notify(keyTransposition.apply(key, value), value);
        }
      }).subscribe(key, firehose);
    }

    @Override
    public <K1 extends Key> void downstream(BiFunction<K, V, K1> keyTransposition,
                                            Subscriber<Tuple2<K1, V>> publisher) {
      upstreamPipe.consume(new KeyedConsumer<K, V>() {
        @Override
        public void accept(K key, V value) {
          publisher.onNext(Tuple.of(keyTransposition.apply(key, value), value));
        }
      }).subscribe(key, firehose);
    }
  }
}
