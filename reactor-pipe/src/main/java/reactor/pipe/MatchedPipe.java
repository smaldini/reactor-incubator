package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;

import javax.lang.model.type.NullType;
import java.util.LinkedList;
import java.util.List;

public class MatchedPipe<V> extends FinalizedMatchedStream<V> {

  protected MatchedPipe() {
    super(TreePVector.empty());
  }

  protected MatchedPipe(PVector<StreamSupplier> suppliers) {
    super(suppliers);
  }

  // TODO: add map with key

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<V1> map(Function<V, V1> mapper) {
    return next(new StreamSupplier<Key, V, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V1> pipe) {
        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<V1> map(Supplier<Function<V, V1>> supplier) {
    return next(new StreamSupplier<Key, V, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V1> pipe) {
        Function<V, V1> mapper = supplier.get();
        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <ST, V1> MatchedPipe<V1> map(BiFunction<Atom<ST>, V, V1> mapper,
                                      ST init) {
    return next(new StreamSupplier<Key, V, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
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
  public MatchedPipe<V> filter(Predicate<V> predicate) {
    return next(new StreamSupplier<Key, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        return (key, value) -> {
          if (predicate.test(value)) {
            pipe.notify(dst.clone(key), value);
          }
        };
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<List<V>> slide(UnaryOperator<List<V>> drop) {
    return next(new StreamSupplier<Key, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        Atom<PVector<V>> buffer = pipe.stateProvider().makeAtom(src, TreePVector.empty());

        return new SlidingWindowOperation<Key, DST, V>(pipe.firehose(),
                                                       buffer,
                                                       drop,
                                                       dst);
      }
    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<List<V>> partition(Predicate<List<V>> emit) {
    return next(new StreamSupplier<Key, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        Atom<PVector<V>> buffer = pipe.stateProvider().makeAtom(dst, TreePVector.empty());

        return new PartitionOperation<Key, DST, V>(pipe.firehose(),
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
  public <SRC extends Key, T> FinalizedMatchedStream<T> consume(KeyedConsumer<SRC, V> consumer) {
    return end(new StreamSupplier<SRC, V, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<NullType> pipe) {
        return consumer;
      }

    });
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, T> FinalizedMatchedStream<T> consume(Consumer<V> consumer) {
    return end(new StreamSupplier<SRC, V, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<NullType> pipe) {
        return (key, value) -> consumer.accept(value);
      }
    });
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, T> FinalizedMatchedStream<T> consume(Supplier<KeyedConsumer<SRC, V>> consumerSupplier) {
    return end(new StreamSupplier<SRC, V, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
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

  public static <V> MatchedPipe<V> build() {
    return new MatchedPipe<>();
  }

}
