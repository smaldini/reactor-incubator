package reactor.pipe.consumer;

// TODO: move to reactor.pipe.consumer
@FunctionalInterface
public interface KeyedConsumer<K, V> {
  void accept(K key, V value);
}
