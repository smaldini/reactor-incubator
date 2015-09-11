package reactor.pipe;

import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

import javax.lang.model.type.NullType;
import java.util.LinkedList;
import java.util.List;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.UnaryOperator;

public class MatchedPipe<V> extends FinalizedMatchedStream<V> {

  public MatchedPipe() {
    super(new LinkedList<>());
  }

  protected MatchedPipe(List<MatchedPipe.StreamSupplier> suppliers) {
    super(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key, V1> MatchedPipe<V1> map(Function<V, V1> mapper) {
    this.suppliers.add(new StreamSupplier<SRC, V, V1>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<V1> pipe) {
        return (key, value) -> {
          pipe.notify(dst, mapper.apply(value));
        };
      }
    });
    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> MatchedPipe<V> filter(Predicate<V> predicate) {
    this.suppliers.add(new StreamSupplier<SRC, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        return (key, value) -> {
          if (predicate.test(value)) {
            pipe.notify(dst, value);
          }
        };
      }
    });

    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> MatchedPipe<List<V>> slide(UnaryOperator<List<V>> drop) {
    this.suppliers.add(new StreamSupplier<SRC, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        Atom<PVector<V>> buffer = pipe.stateProvider().makeAtom(src, TreePVector.empty());

        return new SlidingWindowOperation<SRC, DST, V>(pipe.firehose(),
                                                       buffer,
                                                       drop,
                                                       dst);
      }
    });

    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <SRC extends Key> MatchedPipe<List<V>> partition(Predicate<List<V>> emit) {
    this.suppliers.add(new StreamSupplier<SRC, V, V>() {
      @Override
      public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                         DST dst,
                                                         NamedPipe<V> pipe) {
        Atom<PVector<V>> buffer = pipe.stateProvider().makeAtom(src, TreePVector.empty());

        return new PartitionOperation<SRC, DST, V>(pipe.firehose(),
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

  @FunctionalInterface
  public static interface StreamSupplier<SRC, V, V1> {
    public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                       DST dst,
                                                       NamedPipe<V1> pipe);
  }


}
