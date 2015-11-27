package reactor.pipe.registry;

import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.selector.Selector;

import java.util.List;
import java.util.Map;
import reactor.fn.Function;
import reactor.fn.Predicate;
import java.util.stream.Stream;

public interface Registry<K> extends Iterable<Registration<K>> {

  <V extends KeyedConsumer> Registration<K> register(K sel, V obj);
  void register(Selector<K> matcher,
                Function<K, Map<K, KeyedConsumer>> supplier);

  boolean unregister(K key);
  boolean unregister(Predicate<K> key);
  boolean unregister(Selector<K> key);

  List<Registration<K>> select(K key);

  void clear();

  Stream<Registration<K>> stream();
}
