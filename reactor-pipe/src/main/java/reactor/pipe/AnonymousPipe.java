package reactor.pipe;

import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.List;


public class AnonymousPipe<V> {

  private final Key       rootKey;
  private final Key       upstream;
  private final NamedPipe pipe;

  public AnonymousPipe(Key upstream, NamedPipe pipe) {
    this.upstream = upstream;
    this.rootKey = upstream;
    this.pipe = pipe;
  }

  public AnonymousPipe(Key rootKey, Key upstream, NamedPipe pipe) {
    this.upstream = upstream;
    this.pipe = pipe;
    this.rootKey = rootKey;
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1> AnonymousPipe<V1> map(Function<V, V1> mapper) {
    Key downstream = upstream.derive();

    pipe.map(upstream, downstream, mapper);

    return new AnonymousPipe<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public <V1, ST> AnonymousPipe<V1> map(BiFunction<Atom<ST>, V, V1> mapper,
                                        ST init) {
    Key downstream = upstream.derive();

    pipe.map(upstream, downstream, mapper, init);

    return new AnonymousPipe<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousPipe<V> filter(Predicate<V> predicate) {
    Key downstream = upstream.derive();

    pipe.filter(upstream, downstream, predicate);

    return new AnonymousPipe<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousPipe<List<V>> slide(UnaryOperator<List<V>> drop) {
    Key downstream = upstream.derive();

    pipe.slide(upstream, downstream, drop);

    return new AnonymousPipe<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousPipe<List<V>> partition(Predicate<List<V>> emit) {
    Key downstream = upstream.derive();

    pipe.partition(upstream, downstream, emit);

    return new AnonymousPipe<>(rootKey, downstream, pipe);
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousPipe<V> consume(Consumer<V> consumer) {
    pipe.consume(upstream, consumer);
    return this;
  }

  @SuppressWarnings(value = {"unchecked"})
  public AnonymousPipe<V> redirect(Key destination) {
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
