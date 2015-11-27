package reactor.pipe.registry;

import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.selector.Selector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentRegistry<K> implements Registry<K> {

  private final Atom<PMap<K, PVector<Registration<K>>>>                       lookupMap;
  // This one can't be map, since key miss matcher is a possibly non-capturing lambda,
  // So we have no other means to work around the uniqueness
  private final List<Tuple2<Selector<K>, Function<K, Map<K, KeyedConsumer>>>> keyMissMatchers;

  public ConcurrentRegistry() {
    this.lookupMap = new Atom<>(HashTreePMap.empty());
    this.keyMissMatchers = new ArrayList<>();
  }

  @Override
  public void register(Selector<K> matcher, Function<K, Map<K, KeyedConsumer>> supplier) {
    this.keyMissMatchers.add(Tuple.of(matcher, supplier));
    lookupMap.deref().keySet().forEach(k -> {
      if (matcher.test(k)) {
        supplier.apply(k).forEach((kk, consumer) -> {
          register(kk, consumer);
        });
      }
    });
  }

  @Override
  public <V extends KeyedConsumer> Registration<K> register(final K obj, final V handler) {
    final PVector<Registration<K>> lookedUpArr = lookupMap.deref().get(obj);

    if (lookedUpArr == null) {
      final Registration<K> reg = new SimpleRegistration<>(obj,
                                                           handler,
                                                           // TODO: FIX UNREGISTER
                                                           new Consumer<Registration<K>>() {
                                                             @Override
                                                             public void accept(Registration<K> reg1) {
                                                               lookupMap.deref()
                                                                        .get(obj)
                                                                        .remove(handler);
                                                             }
                                                           });
      final PVector<Registration<K>> emptyArr = TreePVector.singleton(reg);

      lookupMap.update(new UnaryOperator<PMap<K, PVector<Registration<K>>>>() {
        @Override
        public PMap<K, PVector<Registration<K>>> apply(PMap<K, PVector<Registration<K>>> old) {
          return old.plus(obj, emptyArr);
        }
      });

      return reg;

    } else {
      final Registration<K> reg = new SimpleRegistration<>(obj,
                                                           handler,
                                                           // TODO: FIX REMOVES!!
                                                           new Consumer<Registration<K>>() {
                                                             @Override
                                                             public void accept(Registration<K> reg1) {
                                                               lookupMap.deref()
                                                                        .get(obj)
                                                                        .remove(reg1);
                                                             }
                                                           });

      lookupMap.update(new UnaryOperator<PMap<K, PVector<Registration<K>>>>() {
        @Override
        public PMap<K, PVector<Registration<K>>> apply(PMap<K, PVector<Registration<K>>> old) {
          return old.plus(obj, old.get(obj).plus(reg));
        }
      });

      return reg;
    }

  }


  @Override
  public boolean unregister(K key) {
    return lookupMap.updateAndReturnOther((PMap<K, PVector<Registration<K>>> map) -> {
      PMap<K, PVector<Registration<K>>> newv = map.minus(key);

      return Tuple.of(newv,
                      map.containsKey(key));
    });
  }

  @Override
  public boolean unregister(Predicate<K> pred) {
    return lookupMap.updateAndReturnOther((map) -> {
      List<K> unsubscribeKys = map.keySet()
                                  .stream()
                                  .filter(pred::test)
                                  .collect(Collectors.toList());

      PMap<K, PVector<Registration<K>>> newv = map.minusAll(unsubscribeKys);

      return Tuple.of(newv,
                      !unsubscribeKys.isEmpty());
    });
  }

  @Override
  public List<Registration<K>> select(final K key) {
    return
      lookupMap.update(old -> {
        if (old.containsKey(key)) {
          return old;
        } else {
          return keyMissMatchers.stream()
                                .filter((m) -> {
                                  return m.getT1().test(key);
                                })
                                .map(
                                  (Tuple2<Selector<K>, Function<K, Map<K, KeyedConsumer>>> m) -> {
                                    return m.getT2();
                                  })
                                .flatMap((Function<K, Map<K, KeyedConsumer>> m) -> {
                                  return m.apply(key).entrySet().stream();
                                })
                                .reduce(old,
                                        (PMap<K, PVector<Registration<K>>> acc,
                                         Map.Entry<K, KeyedConsumer> entry) -> {
                                          Registration<K> reg = new SimpleRegistration<K, KeyedConsumer>(
                                            entry.getKey(),
                                            entry.getValue(),
                                            // TODO: Fix removes!
                                            null);

                                          if (acc.containsKey(entry.getKey())) {
                                            return acc.plus(entry.getKey(),
                                                            acc.get(entry.getKey()).plus(reg));
                                          } else {
                                            return acc.plus(entry.getKey(),
                                                            TreePVector.singleton(reg));
                                          }
                                        },
                                        PMap::plusAll);


        }
        // TODO: Also, what's stupid, we _always_ create Registration here, even if we could successfully do without
      }).getOrDefault(key, TreePVector.singleton(new Registration<K>() { // Needs a WARN?
        @Override
        public K getSelector() {
          return key;
        }

        @Override
        public KeyedConsumer getObject() {
          return (k, v) -> {
          };
        }
      }));

  }


  @Override
  public void clear() {
    // TODO: FIXME
    //    this.lookupMap.clear();
    //    this.keyMissMatchers.clear();
  }


  @Override
  public Iterator<Registration<K>> iterator() {
    return this.stream().iterator();
  }

  public Stream<Registration<K>> stream() {
    return this.lookupMap.deref().values().stream().flatMap(i -> i.stream());


  }
}
