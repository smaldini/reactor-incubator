package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Predicate;
import reactor.fn.UnaryOperator;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.state.StateProvider;
import reactor.pipe.state.StatefulSupplier;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

// TODO: change named pipe to be just a builder
public class NamedPipe<INIT, CURRENT> {

  private final PVector<StreamBuilder> suppliers;
  private final StateProvider<Key>     stateProvider;

  protected NamedPipe() {
    this(TreePVector.empty(), new DefaultStateProvider<>());
  }

  protected NamedPipe(PVector<StreamBuilder> suppliers,
                      StateProvider<Key> stateProvider) {
    this.suppliers = suppliers;
    this.stateProvider = stateProvider;
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<INIT, List<CURRENT>> partition(SRC source,
                                                                                     DST destination,
                                                                                     Predicate<List<CURRENT>> emit) {
    Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(source, TreePVector.empty());

    return next((Firehose firehose) -> {
      firehose.on(source,
                  new PartitionOperation<SRC, DST, CURRENT>(firehose,
                                                            buffer,
                                                            emit,
                                                            destination));
    };
  }

  );


  //return new NamedPipe<>(firehose);

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<INIT, CURRENT> divide(SRC source,
                                                                            BiFunction<SRC, CURRENT, DST> divider) {

    return next((Firehose firehose) -> {
      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          firehose.notify(divider.apply(key, value), value);
        }
      });
    });

  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<INIT, List<CURRENT>> slide(SRC source,
                                                                                 DST destination,
                                                                                 UnaryOperator<List<CURRENT>> drop) {
    return next((Firehose firehose) -> {
      Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(source, TreePVector.empty());

      firehose.on(source, new SlidingWindowOperation<SRC, DST, CURRENT>(firehose,
                                                                        buffer,
                                                                        drop,
                                                                        destination));
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, NEXT> NamedPipe<INIT, NEXT> map(SRC source,
                                                                            DST destination,
                                                                            Function<CURRENT, NEXT> mapper) {
    return next((Firehose firehose) -> {
      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          firehose.notify(destination.clone(key), mapper.apply(value));
        }
      });
    });

  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, NEXT, ST> NamedPipe<INIT, NEXT> map(SRC source,
                                                                                DST destination,
                                                                                BiFunction<Atom<ST>, CURRENT, NEXT> fn,
                                                                                ST init) {
    Atom<ST> st = stateProvider.makeAtom(source, init);

    return next((Firehose firehose) -> {
      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          firehose.notify(destination.clone(key),
                          fn.apply(st, value));
        }
      });
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key, NEXT, ST> NamedPipe<INIT, NEXT> map(SRC source,
                                                                                DST destination,
                                                                                StatefulSupplier<ST, Function<CURRENT, NEXT>> supplier,
                                                                                ST init) {
    Function<CURRENT, NEXT> mapper = supplier.get(stateProvider.makeAtom(source, init));

    return next((Firehose firehose) -> {
      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          firehose.notify(destination.clone(key), mapper.apply(value));
        }
      });
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<INIT, CURRENT> filter(SRC source,
                                                                            DST destination,
                                                                            Predicate<CURRENT> predicate) {
    return next((Firehose firehose) -> {
      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          if (predicate.test(value)) {
            firehose.notify(destination.clone(key), value);
          }
        }
      });
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, DST extends Key> NamedPipe<INIT, CURRENT> debounce(SRC source,
                                                                              DST destination,
                                                                              int period,
                                                                              TimeUnit timeUnit) {
    final Atom<CURRENT> debounced = stateProvider.makeAtom(source, null);

    return next((Firehose firehose) -> {
      firehose().getTimer().submit(discarded_ -> {
        CURRENT currentDebounced = debounced.updateAndReturnOld(old_ -> null);
        if (currentDebounced != null) {
          // TODO: FIX METADATA!
          firehose.notify(destination, currentDebounced);
        }
      }, period, timeUnit);

      firehose.on(source, new KeyedConsumer<SRC, CURRENT>() {
        @Override
        public void accept(SRC key, CURRENT value) {
          debounced.update(discardedOld_ -> value);
        }
      });
    });

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
  public <SRC extends Key> AnonymousPipe<CURRENT> anonymous(SRC source) {
    return new AnonymousPipe<>(source, this);
  }

  // TODO: remove
  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, TO> void matched(KeyMissMatcher<SRC> keyMatcher,
                                            FinalizedMatchedPipe<CURRENT, TO> downstream) {
    firehose.miss(keyMatcher, downstream.subscribers(firehose));
  }

  @SuppressWarnings(value = {"unchecked"})
  public Channel<CURRENT> channel() {
    Key k = new Key(new Object[]{UUID.randomUUID()});
    AnonymousPipe<CURRENT> anonymousPipe = new AnonymousPipe<>(k,
                                                               this);
    return new Channel<CURRENT>(anonymousPipe,
                                stateProvider.makeAtom(k, TreePVector.empty()));
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> void notify(SRC src, CURRENT v) {
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

  public StateProvider<Key> stateProvider() {
    return this.stateProvider;
  }


  // TODO: last()
  // TODO: first()

  private <NEXT> NamedPipe<INIT, NEXT> next(StreamBuilder supplier) {
    return new NamedPipe<>(suppliers.plus(supplier),
                           stateProvider);
  }

  @FunctionalInterface
  public interface StreamBuilder {
    public void get(Firehose firehose);
  }

}
