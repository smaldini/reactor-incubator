package reactor.pipe;


import org.pcollections.PVector;
import reactor.fn.Function;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.KeyMissMatcher;
import reactor.pipe.state.StateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FinalizedMatchedPipe represents a stream builder that can take values
 * of `INIT` and transform them via the pipeline to `FINAL` type.
 */
public class PipeEnd<INIT, FINAL> implements IPipe.PipeEnd<INIT, FINAL> {

  private final PVector<StreamSupplier> suppliers;

  protected PipeEnd(PVector<StreamSupplier> suppliers) {
    this.suppliers = suppliers;
  }


  @Override
  public void subscribe(Key key, Firehose firehose) {
    Key currentKey = key;
    for (StreamSupplier supplier : suppliers) {
      Key nextKey = currentKey.derive();
      firehose.on(currentKey, supplier.get(currentKey, nextKey, firehose));
      currentKey = nextKey;
    }
  }

  @Override
  public <K extends Key> void subscribe(KeyMissMatcher<K> matcher, Firehose firehose) {
    firehose.miss(matcher,
                  subscribers(firehose));
  }

  private Function<Key, Map<Key, KeyedConsumer<? extends Key, FINAL>>> subscribers(Firehose firehose) {
    return new Function<Key, Map<Key, KeyedConsumer<? extends Key, FINAL>>>() {
      @Override
      public Map<Key, KeyedConsumer<? extends Key, FINAL>> apply(Key key) {
        Map<Key, KeyedConsumer<? extends Key, FINAL>> consumers = new LinkedHashMap<>();

        Key currentKey = key;
        for (StreamSupplier supplier : suppliers) {
          Key nextKey = currentKey.derive();
          consumers.put(currentKey, supplier.get(currentKey, nextKey, firehose));
          currentKey = nextKey;
        }
        return consumers;
      }
    };
  }
}
