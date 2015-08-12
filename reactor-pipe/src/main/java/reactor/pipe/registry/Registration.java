package reactor.pipe.registry;

import reactor.pipe.KeyedConsumer;

public interface Registration<K> {

  K getSelector(); // TODO: Rename to getKey, since we don't really have selectors in their old meaning

  <V> KeyedConsumer<K, V> getObject();

  // cancelAfterUse?
}
