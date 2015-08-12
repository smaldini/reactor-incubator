package reactor.pipe;


import reactor.pipe.key.Key;

import java.util.*;
import java.util.function.Function;

class FinalizedMatchedStream<V> {

  protected final List<MatchedPipe.StreamSupplier> suppliers;

  protected FinalizedMatchedStream(List<MatchedPipe.StreamSupplier> suppliers) {
    this.suppliers = suppliers;
  }

  public Function<Key, Map<Key, KeyedConsumer<? extends Key, V>>> subscribers(Pipe pipe) {
    return new Function<Key, Map<Key, KeyedConsumer<? extends Key, V>>>() {
      @Override
      public Map<Key, KeyedConsumer<? extends Key, V>> apply(Key key) {
        Map<Key, KeyedConsumer<? extends Key, V>> consumers = new LinkedHashMap<>();

        Key currentKey = key;
        for (MatchedPipe.StreamSupplier supplier : suppliers) {
          Key nextKey = currentKey.derive();
          consumers.put(currentKey, supplier.get(currentKey, nextKey, pipe));
          currentKey = nextKey;
        }
        return consumers;
      }
    };

  }
}
