package reactor.pipe.registry;

import reactor.pipe.consumer.KeyedConsumer;

import java.util.Map;
import reactor.fn.Function;
import reactor.pipe.selector.Selector;

public interface DefaultingRegistry<K> extends Registry<K>, Iterable<Registration<K>> {

  void addKeyMissMatcher(Selector<K> matcher,
                         Function<K, Map<K, KeyedConsumer>> supplier);

}
