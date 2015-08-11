package reactor.pipe.registry;

import reactor.pipe.KeyedConsumer;
import reactor.pipe.key.Key;

import java.util.Map;
import java.util.function.Function;

public interface DefaultingRegistry<K extends Key> extends Registry<K>, Iterable<Registration<K>> {

  public void addKeyMissMatcher(KeyMissMatcher<K> matcher,
                                Function<K, Map<K, KeyedConsumer>> supplier);

}
