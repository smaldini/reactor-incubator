package reactor.pipe.registry;

import reactor.pipe.KeyedConsumer;
import reactor.pipe.key.Key;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface Registry<K extends Key> {

  <V extends KeyedConsumer> Registration<K> register(K sel, V obj);

  boolean unregister(K key);

  boolean unregister(Predicate<K> key);

  List<Registration<K>> select(K key);

  void clear();

  Stream<Registration<K>> stream();
}
