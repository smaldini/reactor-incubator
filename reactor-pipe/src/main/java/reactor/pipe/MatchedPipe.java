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

  MatchedPipe() {
    super(new LinkedList<>());
  }

  protected MatchedPipe(List<MatchedPipe.StreamSupplier> suppliers) {
    super(suppliers);
  }

  // TODO: add map with key

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<V1> map(Function<V, V1> mapper) {
    this.suppliers.add(new StreamSupplier<Key, V, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<Key, V> get(Key src,
                                                         DST dst,
                                                         NamedPipe<V1> pipe) {
        return (key, value) -> {
          pipe.notify(dst.clone(key), mapper.apply(value));
        };
      }
    });
    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<V1> map(Supplier<Function<V, V1>> supplier) {
    this.suppliers.add(new StreamSupplier<Key, V, V1>() {
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
    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <ST, V1> MatchedPipe<V1> map(BiFunction<Atom<ST>, V, V1> mapper,
                                      ST init) {
    this.suppliers.add(new StreamSupplier<Key, V, V1>() {
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
    return new MatchedPipe<>(suppliers);
  }


  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<V> filter(Predicate<V> predicate) {
    this.suppliers.add(new StreamSupplier<Key, V, V>() {
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

    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<List<V>> slide(UnaryOperator<List<V>> drop) {
    this.suppliers.add(new StreamSupplier<Key, V, V>() {
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

    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<List<V>> partition(Predicate<List<V>> emit) {
    this.suppliers.add(new StreamSupplier<Key, V, V>() {
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

    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedStream consume(KeyedConsumer<SRC, V> consumer) {
    this.suppliers.add(new StreamSupplier<SRC, V, NullType>() {

      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<NullType> pipe) {
        return consumer;
      }

    });
    return new FinalizedMatchedStream(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedStream consume(Consumer<V> consumer) {
    this.suppliers.add(new StreamSupplier<SRC, V, NullType>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<NullType> pipe) {
        return (key, value) -> consumer.accept(value);
      }
    });
    return new FinalizedMatchedStream(suppliers);
  }


  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> FinalizedMatchedStream consume(Supplier<KeyedConsumer<SRC, V>> consumerSupplier) {
    this.suppliers.add(new StreamSupplier<SRC, V, NullType>() {

      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<NullType> pipe) {
        return consumerSupplier.get();
      }

    });
    return new FinalizedMatchedStream(suppliers);
  }

  @FunctionalInterface
  public static interface StreamSupplier<SRC extends Key, V, V1> {
    public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                       DST dst,
                                                       NamedPipe<V1> pipe);
  }


}
