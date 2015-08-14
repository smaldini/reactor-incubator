package reactor.pipe;

import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import reactor.pipe.registry.KeyMissMatcher;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.state.StateProvider;
import reactor.pipe.state.StatefulSupplier;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public class NamedPipe<V> {

  private final Firehose      firehose;
  private final StateProvider stateProvider;

  public NamedPipe() {
    this(new Firehose());
  }


  protected NamedPipe(Firehose firehose) {
    this.firehose = firehose;
    this.stateProvider = new DefaultStateProvider();
  }

  public NamedPipe<V> fork(ExecutorService executorService,
                        int ringBufferSize) {
    return new NamedPipe<V>(firehose.fork(executorService,
                                       ringBufferSize));
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<List<V>> partition(SRC source,
                                                                      DST destination,
                                                                      Predicate<List<V>> emit) {
    Atom<PVector<V>> buffer = stateProvider.makeAtom(source, TreePVector.empty());

    firehose.on(source, new PartitionOperation<SRC, DST, V>(firehose,
                                                            buffer,
                                                            emit,
                                                            destination));

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<V> divide(SRC source,
                                                             BiFunction<SRC, V, DST> divider) {
    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        firehose.notify(divider.apply(key, value), value);
      }
    });

    return new NamedPipe<>(firehose);
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<List<V>> slide(SRC source,
                                                                  DST destination,
                                                                  UnaryOperator<List<V>> drop) {
    Atom<PVector<V>> buffer = stateProvider.makeAtom(source, TreePVector.empty());

    firehose.on(source, new SlidingWindowOperation<SRC, DST, V>(firehose,
                                                                buffer,
                                                                drop,
                                                                destination));

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, V1> NamedPipe<V1> map(SRC source,
                                                               DST destination,
                                                               Function<V, V1> mapper) {
    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        firehose.notify(destination, mapper.apply(value));
      }
    });

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, V1, ST> NamedPipe<V1> map(SRC source,
                                                                   DST destination,
                                                                   BiFunction<Atom<ST>, V, V1> fn,
                                                                   ST init) {
    Atom<ST> st = stateProvider.makeAtom(source, init);

    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        firehose.notify(destination, fn.apply(st, value));
      }
    });

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, V1, ST> NamedPipe<V1> map(SRC source,
                                                                   DST destination,
                                                                   StatefulSupplier<ST, Function<V, V1>> supplier,
                                                                   ST init) {
    Function<V, V1> mapper = supplier.get(stateProvider.makeAtom(source, init));

    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        firehose.notify(destination, mapper.apply(value));
      }
    });

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<V> filter(SRC source,
                                                             DST destination,
                                                             Predicate<V> predicate) {
    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        if (predicate.test(value)) {
          firehose.notify(destination, value);
        }
      }
    });

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<V> debounce(SRC source,
                                                               DST destination,
                                                               int period,
                                                               TimeUnit timeUnit) {
    final Atom<V> debounced = stateProvider.makeAtom(source, null);

    firehose.getTimer().schedule(discarded_ -> {
      V currentDebounced = debounced.updateAndReturnOld(old_ -> null);
      if (currentDebounced != null) {
        firehose.notify(destination, currentDebounced);
      }
    }, period, timeUnit);

    firehose.on(source, new KeyedConsumer<SRC, V>() {
      @Override
      public void accept(SRC key, V value) {
        debounced.update(discardedOld_ -> value);
      }
    });

    return new NamedPipe<>(firehose);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, V1> void consume(SRC source,
                                            Consumer<V1> consumer) {
    this.consume(source,
                 new KeyedConsumer<SRC, V1>() {
                   @Override
                   public void accept(SRC key_, V1 value) {
                     consumer.accept(value);
                   }
                 });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, V1> void consume(SRC source,
                                            KeyedConsumer<SRC, V1> consumer) {
    firehose.on(source, consumer);
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> AnonymousPipe<V> anonymous(SRC source) {
    return new AnonymousPipe<>(source, this);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> MatchedPipe<V> matched(KeyMissMatcher<SRC> keyMatcher) {
    MatchedPipe<V> downstream = new MatchedPipe<>();
    firehose.miss(keyMatcher, downstream.subscribers(this));
    return downstream;
  }

  @SuppressWarnings(value = {"unchecked"})
  public Channel<V> channel() {
    Key k = new Key(new Object[]{UUID.randomUUID()});
    AnonymousPipe<V> anonymousPipe = new AnonymousPipe<>(k,
                                                               this);
    return new Channel<V>(anonymousPipe,
                          stateProvider.makeAtom(k, TreePVector.empty()));
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> void notify(SRC src, V v) {
    this.firehose.notify(src, v);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> void unregister(SRC src) {
    this.firehose.unregister(src);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> void unregister(Predicate<SRC> pred) {
    this.firehose.unregister(pred);
  }

  public Firehose firehose() {
    return this.firehose;
  }

  public StateProvider stateProvider() {
    return this.stateProvider;
  }



  // TODO: last()
  // TODO: first()

}
