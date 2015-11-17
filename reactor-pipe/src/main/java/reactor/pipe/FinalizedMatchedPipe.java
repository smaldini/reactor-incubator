package reactor.pipe;


import org.pcollections.PVector;
import reactor.fn.Function;
import reactor.pipe.key.Key;
import reactor.pipe.state.StateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FinalizedMatchedPipe represents a stream builder that can take values
 * of `INIT` and transform them via the pipeline to `FINAL` type.
 */
public class FinalizedMatchedPipe<INIT, FINAL> {

  protected final StateProvider<Key>      stateProvider;
  protected final PVector<StreamSupplier> suppliers;

  protected FinalizedMatchedPipe(PVector<StreamSupplier> suppliers,
                                 StateProvider<Key> stateProvider) {
    this.suppliers = suppliers;
    this.stateProvider = stateProvider;
  }

  protected <NEXT> MatchedPipe<INIT, NEXT> next(StreamSupplier supplier) {
    return new MatchedPipe<>(suppliers.plus(supplier),
                             stateProvider);
  }

  protected <NEXT> FinalizedMatchedPipe<INIT, NEXT> end(StreamSupplier supplier) {
    return new FinalizedMatchedPipe<>(suppliers.plus(supplier),
                                      stateProvider);
  }

  public Function<Key, Map<Key, KeyedConsumer<? extends Key, FINAL>>> subscribers(Firehose pipe) {
    return new Function<Key, Map<Key, KeyedConsumer<? extends Key, FINAL>>>() {
      @Override
      public Map<Key, KeyedConsumer<? extends Key, FINAL>> apply(Key key) {
        Map<Key, KeyedConsumer<? extends Key, FINAL>> consumers = new LinkedHashMap<>();

        Key currentKey = key;
        for (StreamSupplier supplier : suppliers) {
          Key nextKey = currentKey.derive();
          consumers.put(currentKey, supplier.get(currentKey, nextKey, pipe));
          currentKey = nextKey;
        }
        return consumers;
      }
    };

  }
}
