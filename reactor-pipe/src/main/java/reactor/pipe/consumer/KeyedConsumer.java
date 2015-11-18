package reactor.pipe.consumer;

// TODO: move to reactor.pipe.consumer
@FunctionalInterface
public interface KeyedConsumer<K, V> {

  public void accept(K key, V value);

}
