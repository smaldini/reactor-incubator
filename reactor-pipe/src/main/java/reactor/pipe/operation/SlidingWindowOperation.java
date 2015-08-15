package reactor.pipe.operation;

import reactor.pipe.Firehose;
import reactor.pipe.KeyedConsumer;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

import java.util.List;
import reactor.fn.UnaryOperator;

public class SlidingWindowOperation<SRC extends Key, DST extends Key, V> implements KeyedConsumer<SRC, V> {

  private final Atom<PVector<V>>       buffer;
  private final Firehose               firehose;
  private final UnaryOperator<List<V>> drop;
  private final DST                    destination;

  public SlidingWindowOperation(Firehose firehose,
                                Atom<PVector<V>> buffer,
                                UnaryOperator<List<V>> drop,
                                DST destination) {
    this.buffer = buffer;
    this.firehose = firehose;
    this.drop = drop;
    this.destination = destination;
  }

  @Override
  public void accept(SRC key, V value) {
    PVector<V> newv = buffer.update((old) -> {
      List<V> dropped = drop.apply(old.plus(value));
      return TreePVector.from(dropped);
    });

    firehose.notify(destination, newv);
  }
}
