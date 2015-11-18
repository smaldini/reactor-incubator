package reactor.pipe.stream;


import reactor.pipe.Firehose;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;

@FunctionalInterface
public interface StreamSupplier<K extends Key, V> {
  public KeyedConsumer<K, V> get(K src,
                                 Key dst,
                                 Firehose firehose);
}
