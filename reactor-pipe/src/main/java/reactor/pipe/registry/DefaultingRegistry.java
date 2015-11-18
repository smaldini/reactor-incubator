package reactor.pipe.registry;

import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;

import java.util.Map;
import reactor.fn.Function;

public interface DefaultingRegistry<K extends Key> extends Registry<K>, Iterable<Registration<K>> {

  public void addKeyMissMatcher(KeyMissMatcher<K> matcher,
                                Function<K, Map<K, KeyedConsumer>> supplier);

}
