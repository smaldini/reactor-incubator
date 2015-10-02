package reactor.pipe.redis;

import reactor.pipe.codec.Codec;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.state.StateProvider;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RedisStateProvider<K> implements StateProvider<K> {

  private final Map<Class, Codec>        encoders;
  private final ScheduledExecutorService executor;
  private final JedisPool                pool;

  public RedisStateProvider(String host, int port) {
    this.encoders = new HashMap<>();
    executor = Executors.newScheduledThreadPool(1);
    pool = new JedisPool(new JedisPoolConfig(),
                         host,
                         port);
  }

  public <T> void registerCodec(Class<T> klass,
                                Codec<T, String> encoder) {
    this.encoders.put(klass, encoder);
  }

  @SuppressWarnings("unchecked")
  protected <IN, OUT> Codec<IN, OUT> getCodec(Class<IN> klass) {
    return (Codec<IN, OUT>) this.encoders.get(klass);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Atom<T> makeAtom(K src, T init) { // Init should never be null!
    Codec<K, String> keyCodec = getCodec((Class<K>) src.getClass());
    Codec<T, String> valueCodec = getCodec((Class<T>) init.getClass());

    Atom<T> atom = new Atom<>(initialLoad(src, init));

    executor.submit(new Runnable() {
      @Override
      public void run() {
        try (Jedis client = pool.getResource()) {
          client.set(keyCodec.encode(src),
                     valueCodec.encode(atom.deref()));
        } catch (Exception e) {
          //
        }

      }
    });

    return atom;
  }

  @Override
  public <T> Atom<T> makeAtom(T init) {
    return new Atom<T>(init);
  }

  @SuppressWarnings("unchecked")
  protected <T> T initialLoad(K src, T init) {
    Codec<K, String> keyCodec = getCodec((Class<K>) src.getClass());
    Codec<T, String> valueCodec = getCodec((Class<T>) init.getClass());

    try (Jedis client = pool.getResource()) {
      String loaded = client.get(keyCodec.encode(src));
      if (loaded == null) {
        return init;
      } else {
        return valueCodec.decode(loaded);
      }
    } catch (Exception e) {
      throw e;
    }
  }
}
