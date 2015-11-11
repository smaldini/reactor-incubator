package reactor.pipe;


import org.pcollections.PVector;
import reactor.pipe.key.Key;

import java.util.*;
import reactor.fn.Function;

public class FinalizedMatchedPipe<FROM, V> {

  protected final PVector<MatchedPipe.StreamSupplier> suppliers;

  protected FinalizedMatchedPipe(PVector<MatchedPipe.StreamSupplier> suppliers) {
    this.suppliers = suppliers;
  }

  protected <FROM, V1> MatchedPipe<FROM, V1> next(MatchedPipe.StreamSupplier supplier) {
    return new MatchedPipe<>(suppliers.plus(supplier));
  }

  protected <FROM, V1> FinalizedMatchedPipe<FROM, V1> end(MatchedPipe.StreamSupplier supplier) {
    return new FinalizedMatchedPipe<>(suppliers.plus(supplier));
  }

  public Function<Key, Map<Key, KeyedConsumer<? extends Key, V>>> subscribers(NamedPipe pipe) {
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
