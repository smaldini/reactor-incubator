package reactor.pipe;


import org.pcollections.PVector;
import reactor.fn.Function;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.selector.Selector;
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
  public void subscribe(Key key, Firehose<Key> firehose) {
    Key currentKey = key;
    for (StreamSupplier supplier : suppliers) {
      Key nextKey = currentKey.derive();
      firehose.on(currentKey, supplier.get(currentKey, nextKey, firehose));
      currentKey = nextKey;
    }
  }

  @Override
  public void subscribe(Selector<Key> matcher, Firehose<Key> firehose) {
    firehose.on(matcher,
                subscribers(firehose));
  }

  private Function<Key, Map<Key, KeyedConsumer>> subscribers(Firehose firehose) {
    return new Function<Key, Map<Key, KeyedConsumer>>() {
      @Override
      public Map<Key, KeyedConsumer> apply(Key key) {
        Map<Key, KeyedConsumer> consumers = new LinkedHashMap<>();

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
