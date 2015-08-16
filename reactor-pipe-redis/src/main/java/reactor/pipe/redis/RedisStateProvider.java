package reactor.pipe.redis;

import reactor.pipe.codec.Codec;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.state.StateProvider;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import redis.clients.jedis.*;

public class RedisStateProvider implements StateProvider {

  private final Map<Class, Codec>        encoders;
  private final ScheduledExecutorService executor;
  private final JedisPool                pool;

  public RedisStateProvider() {
    this.encoders = new HashMap<>();
    executor = Executors.newScheduledThreadPool(1);
    pool = new JedisPool(new JedisPoolConfig(),
                         "localhost",
                         6379);
  }

  public <T> void registerCodec(Class<T> klass, Codec<T, String> encoder) {
    this.encoders.put(klass, encoder);
  }

  @SuppressWarnings("unchecked")
  protected <IN, OUT> Codec<IN, OUT> getCodec(Class<IN> klass) {
    return (Codec<IN, OUT>) this.encoders.get(klass);
  }

  @Override
  public <SRC, T> Atom<T> makeAtom(SRC src, T init) { // Init should never be null!
    Codec<SRC, String> keyCodec = getCodec((Class<SRC>) init.getClass());
    Codec<T, String> valueCodec = getCodec((Class<T>) init.getClass());

    Atom<T> atom = new Atom<>(init);

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

  
}
