package reactor.pipe.integration;

import reactor.pipe.SimpleConsumer;

public interface Source<K, V> extends SimpleConsumer<PipeTuple<K,V>> {

}
