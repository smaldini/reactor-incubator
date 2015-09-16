package reactor.pipe;

import org.junit.After;
import org.junit.Before;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractFirehoseTest {

  public static final long     LATCH_TIMEOUT   = 10;
  public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

  protected Firehose<Key>   firehose;
  protected ExecutorService executorService;

  @Before
  public void setup() {
    this.executorService = Executors.newFixedThreadPool(16);
    this.firehose = new Firehose<>(new ConcurrentRegistry<>(),
                                   RingBufferProcessor.create(executorService, 2048),
                                   new Consumer<Throwable>() {
                                     @Override
                                     public void accept(Throwable throwable) {
                                       System.out.printf("Exception caught while dispatching: %s\n", throwable.getMessage());
                                       throwable.printStackTrace();
                                     }
                                   });
  }

  @After
  public void teardown() {
    firehose.shutdown();
    executorService.shutdown();
  }

}
