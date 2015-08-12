package reactor.pipe;

@FunctionalInterface
public interface SimpleConsumer<V> {

  public void accept(V value);

  public static <T> KeyedConsumer<?, T> wrap(final SimpleConsumer<T> consumer) {
    return new KeyedConsumer<Object, T>() {
      @Override
      public void accept(Object k, T value) {
        consumer.accept(value);
      }
    };
  }
}
