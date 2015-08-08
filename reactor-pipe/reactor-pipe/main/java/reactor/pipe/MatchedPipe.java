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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class MatchedPipe<V> extends FinalizedMatchedStream<V> {

  public MatchedPipe() {
    super(new LinkedList<>());
  }

  protected MatchedPipe(List<MatchedPipe.StreamSupplier> suppliers) {
    super(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> MatchedPipe<V1> map(Function<V, V1> mapper) {
    this.suppliers.add(new StreamSupplier<V, V1>() {
      @Override
      public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                          DST dst,
                                                                          Pipe<V1> pipe) {
        return (key, value) -> {
          pipe.notify(dst, mapper.apply(value));
        };
      }
    });
    return new MatchedPipe<>(suppliers);
  }

  @SuppressWarnings(value = {"unchecked"})
  public MatchedPipe<V> filter(Predicate<V> predicate) {
    this.suppliers.add(new StreamSupplier<V, V>() {
      @Override
      public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                          DST dst,
                                                                          Pipe<V> pipe) {
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
  public MatchedPipe<List<V>> slide(UnaryOperator<List<V>> drop) {
    this.suppliers.add(new StreamSupplier<V, V>() {
      @Override
      public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                          DST dst,
                                                                          Pipe<V> pipe) {
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
  public MatchedPipe<List<V>> partition(Predicate<List<V>> emit) {
    this.suppliers.add(new StreamSupplier<V, V>() {
      @Override
      public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                          DST dst,
                                                                          Pipe<V> pipe) {
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
  public FinalizedMatchedStream consume(Consumer<V> consumer) {
    this.suppliers.add(new StreamSupplier<V, NullType>() {
      @Override
      public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                          DST dst,
                                                                          Pipe<NullType> pipe) {
        return (key, value) -> consumer.accept(value);
      }
    });
    return new FinalizedMatchedStream(suppliers);
  }

  @FunctionalInterface
  public static interface StreamSupplier<V, V1> {
    public <SRC extends Key, DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                                        DST dst,
                                                                        Pipe<V1> pipe);
  }


}
