package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;

import javax.lang.model.type.NullType;
import java.util.List;

public class MatchedPipe<FROM, CURRENT> extends FinalizedMatchedPipe<FROM, CURRENT> {

  protected MatchedPipe() {
    super(TreePVector.empty());
  }

  protected MatchedPipe(PVector<StreamSupplier> suppliers) {
    super(suppliers);
  }

  // TODO: add map with key

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<FROM, V1> map(Function<CURRENT, V1> mapper) {
    return next(new StreamSupplier<Key, CURRENT, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<V1> pipe) {
        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<FROM, V1> map(Supplier<Function<CURRENT, V1>> supplier) {
    return next(new StreamSupplier<Key, CURRENT, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<V1> pipe) {
        Function<CURRENT, V1> mapper = supplier.get();
        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <ST, V1> MatchedPipe<FROM, V1> map(BiFunction<Atom<ST>, CURRENT, V1> mapper,
                                      ST init) {
    return next(new StreamSupplier<Key, CURRENT, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<V1> pipe) {
        Atom<ST> st = pipe.stateProvider().makeAtom(src, init);

        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(st, value));
        };
      }
    });
  }


  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<FROM, CURRENT> filter(Predicate<CURRENT> predicate) {
    return next(new StreamSupplier<Key, CURRENT, CURRENT>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<CURRENT> pipe) {
        return (key, value) -> {
          if (predicate.test(value)) {
            pipe.notify(dst.clone(key), value);
          }
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<FROM, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop) {
    return next(new StreamSupplier<Key, CURRENT, CURRENT>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<CURRENT> pipe) {
        Atom<PVector<CURRENT>> buffer = pipe.stateProvider().makeAtom(src, TreePVector.empty());

        return new SlidingWindowOperation<Key, DST, CURRENT>(pipe.firehose(),
                                                             buffer,
                                                             drop,
                                                             dst);
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<FROM, List<CURRENT>> partition(Predicate<List<CURRENT>> emit) {
    return next(new StreamSupplier<Key, CURRENT, CURRENT>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, CURRENT> get(Key src,
                                                               DST dst,
                                                               NamedPipe<CURRENT> pipe) {
        Atom<PVector<CURRENT>> buffer = pipe.stateProvider().makeAtom(dst, TreePVector.empty());

        return new PartitionOperation<Key, DST, CURRENT>(pipe.firehose(),
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
  public <SRC extends Key, TO> FinalizedMatchedPipe<FROM, TO> consume(KeyedConsumer<SRC, CURRENT> consumer) {
    return end(new StreamSupplier<SRC, CURRENT, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, CURRENT> get(SRC src,
                                                               DST dst,
                                                               NamedPipe<NullType> pipe) {
        return consumer;
      }

    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, TO> FinalizedMatchedPipe<FROM, TO> consume(Consumer<CURRENT> consumer) {
    return end(new StreamSupplier<SRC, CURRENT, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, CURRENT> get(SRC src,
                                                               DST dst,
                                                               NamedPipe<NullType> pipe) {
        return (key, value) -> consumer.accept(value);
      }
    });
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, TO> FinalizedMatchedPipe<FROM, TO> consume(Supplier<KeyedConsumer<SRC, CURRENT>> consumerSupplier) {
    return end(new StreamSupplier<SRC, CURRENT, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, CURRENT> get(SRC src,
                                                               DST dst,
                                                               NamedPipe<NullType> pipe) {
        return consumerSupplier.get();
      }

    });
  }

  @FunctionalInterface
  public static interface StreamSupplier<SRC extends Key, V, V1> {
    public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                       DST dst,
                                                       NamedPipe<V1> pipe);
  }

  public static <V> MatchedPipe<V, V> build() {
    return new MatchedPipe<>();
  }

}
