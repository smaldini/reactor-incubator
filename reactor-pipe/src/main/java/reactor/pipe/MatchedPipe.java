package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.state.StateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.List;

/**
 * Matched pipe represents a (possibly multi-step) transformation from `INIT` type,
 * which is an initial type of the topolgy, to the `CURRENT` type.
 */
public class MatchedPipe<INIT, CURRENT> extends FinalizedMatchedPipe<INIT, CURRENT> {

  protected MatchedPipe() {
    this(TreePVector.empty(), new DefaultStateProvider<>());
  }

  protected MatchedPipe(PVector<StreamSupplier> suppliers,
                        StateProvider<Key> stateProvider) {
    super(suppliers, stateProvider);
  }

  // TODO: add map with key

  @SuppressWarnings(value = {"unchecked"})
  public <NEXT> MatchedPipe<INIT, NEXT> map(Function<CURRENT, NEXT> mapper) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        return (key, value) -> {
          firehose.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <NEXT> MatchedPipe<INIT, NEXT> map(Supplier<Function<CURRENT, NEXT>> supplier) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        Function<CURRENT, NEXT> mapper = supplier.get();
        return (key, value) -> {
          firehose.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <ST, NEXT> MatchedPipe<INIT, NEXT> map(BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
                                                ST init) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        Atom<ST> st = stateProvider.makeAtom(src, init);

        return (key, value) -> {
          firehose.notify(dst.clone(key), mapper.apply(st, value));
        };
      }
    });
  }


  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<INIT, CURRENT> filter(Predicate<CURRENT> predicate) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        return (key, value) -> {
          if (predicate.test(value)) {
            firehose.notify(dst.clone(key), value);
          }
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<INIT, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(src, TreePVector.empty());

        return new SlidingWindowOperation<>(firehose,
                                            buffer,
                                            drop,
                                            dst);
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<INIT, List<CURRENT>> partition(Predicate<List<CURRENT>> emit) {
    return next(new StreamSupplier<Key, CURRENT>() {
      @Override
      public KeyedConsumer<Key, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose firehose) {
        Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(dst, TreePVector.empty());

        return new PartitionOperation<>(firehose,
                                        buffer,
                                        emit,
                                        dst);
      }
    });
  }

  /**
   * STREAM ENDS
   */

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedPipe<INIT, CURRENT> consume(KeyedConsumer<SRC, CURRENT> consumer) {
    return end(new StreamSupplier<SRC, CURRENT>() {
      @Override
      public KeyedConsumer<SRC, CURRENT> get(Key src,
                                             Key dst,
                                             Firehose pipe) {
        return consumer;
      }

    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedPipe<INIT, CURRENT> consume(Consumer<CURRENT> consumer) {
    return end(new StreamSupplier<SRC, CURRENT>() {
      @Override
      public KeyedConsumer<SRC, CURRENT> get(SRC src,
                                             Key dst,
                                             Firehose pipe) {
        return (key, value) -> consumer.accept(value);
      }
    });
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedPipe<INIT, CURRENT> consume(
    Supplier<KeyedConsumer<SRC, CURRENT>> consumerSupplier) {
    return end(new StreamSupplier<SRC, CURRENT>() {
      @Override
      public KeyedConsumer<SRC, CURRENT> get(SRC src,
                                             Key dst,
                                             Firehose pipe) {
        return consumerSupplier.get();
      }

    });
  }

  public static <V> MatchedPipe<V, V> build() {
    return new MatchedPipe<>();
  }

}
