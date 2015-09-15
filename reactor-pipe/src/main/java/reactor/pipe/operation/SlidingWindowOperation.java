package reactor.pipe.operation;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.UnaryOperator;
import reactor.pipe.Firehose;
import reactor.pipe.KeyedConsumer;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.List;

public class SlidingWindowOperation<SRC extends Key, DST extends Key, V> implements KeyedConsumer<SRC, V> {

  private final Atom<PVector<V>>       buffer;
  private final Firehose<Key>          firehose;
  private final UnaryOperator<List<V>> drop;
  private final DST                    destination;

  public SlidingWindowOperation(Firehose<Key> firehose,
                                Atom<PVector<V>> buffer,
                                UnaryOperator<List<V>> drop,
                                DST destination) {
    this.buffer = buffer;
    this.firehose = firehose;
    this.drop = drop;
    this.destination = destination;
  }

  @Override
  public void accept(SRC src, V value) {
    PVector<V> newv = buffer.update((old) -> {
      List<V> dropped = drop.apply(old.plus(value));
      return TreePVector.from(dropped);
    });

    firehose.notify(destination.clone(src), newv);
  }
}
