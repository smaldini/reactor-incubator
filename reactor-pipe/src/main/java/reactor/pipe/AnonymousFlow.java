package reactor.pipe;

import reactor.pipe.key.Key;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class AnonymousFlow<V> {

  private final Key  rootKey;
  private final Key  upstream;
  private final Pipe pipe;

  public AnonymousFlow(Key upstream, Pipe pipe) {
    this.upstream = upstream;
    this.rootKey = upstream;
    this.pipe = pipe;
  }

  public AnonymousFlow(Key rootKey, Key upstream, Pipe pipe) {
    this.upstream = upstream;
    this.pipe = pipe;
    this.rootKey = rootKey;
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> AnonymousFlow<V1> map(Function<V, V1> mapper) {
    Key downstream = upstream.derive();

    pipe.map(upstream, downstream, mapper);

    return new AnonymousFlow<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousFlow<V> filter(Predicate<V> predicate) {
    Key downstream = upstream.derive();

    pipe.filter(upstream, downstream, predicate);

    return new AnonymousFlow<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousFlow<List<V>> slide(UnaryOperator<List<V>> drop) {
    Key downstream = upstream.derive();

    pipe.slide(upstream, downstream, drop);

    return new AnonymousFlow<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousFlow<List<V>> partition(Predicate<List<V>> emit) {
    Key downstream = upstream.derive();

    pipe.partition(upstream, downstream, emit);

    return new AnonymousFlow<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousFlow<V> consume(Consumer<V> consumer) {
    pipe.consume(upstream, consumer);
    return this;
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousFlow<V> redirect(Key destination) {
    pipe.consume(upstream, new KeyedConsumer<Key, V>() {
      @Override
      public void accept(Key key, V value) {
        pipe.notify(destination, value);
      }
    });

    return this;
  }

  @SuppressWarnings(value = {"unchecked"})
  public Runnable cancellableConsumer(Consumer<V> consumer) {
    pipe.consume(upstream, consumer);
    return () -> {
      pipe.unregister(upstream);
    };
  }

  @SuppressWarnings(value = {"unchecked"})
  public void notify(V v) {
    this.pipe.notify(upstream, v);
  }

  public void unregister() {
    this.pipe.unregister(new Predicate<Key>() {
      @Override
      public boolean test(Key k) {
        return k.isDerivedFrom(rootKey) || k.equals(rootKey);
      }
    });
  }
}
