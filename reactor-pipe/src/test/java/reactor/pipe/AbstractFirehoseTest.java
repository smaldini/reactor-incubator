package reactor.pipe;

import org.junit.After;
import org.junit.Before;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.fn.Consumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractFirehoseTest {

  public static final long     LATCH_TIMEOUT   = 10;
  public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

  protected Firehose<Key> firehose;

  @Before
  public void setup() {
    this.firehose = new Firehose<>(new ConcurrentRegistry<>(),
                                   RingBufferWorkProcessor.create(Executors.newFixedThreadPool(4),
                                                                  256),
                                   1,
                                   throwable -> {
                                   });
  }

  @After
  public void teardown() {
    firehose.shutdown();
  }

}
