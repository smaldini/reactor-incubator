package reactor.pipe.stream;


import reactor.pipe.Firehose;
import reactor.pipe.KeyedConsumer;
import reactor.pipe.key.Key;

@FunctionalInterface
public interface StreamSupplier<SRC extends Key, V, V1> {
  public <DST extends Key> KeyedConsumer<SRC, V> get(SRC src,
                                                     DST dst,
                                                     Firehose pipe);
}
