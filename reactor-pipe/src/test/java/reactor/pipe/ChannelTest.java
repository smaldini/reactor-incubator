package reactor.pipe;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.pipe.concurrent.AVar;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ChannelTest extends AbstractStreamTest {

  @Test
  public void simpleChannelTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    chan.tell(1);
    chan.tell(2);

    assertThat(chan.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(1));
    assertThat(chan.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(2));
    assertTrue(chan.get() == null);

    chan.tell(3);
    assertThat(chan.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(3));
  }

  @Test
  public void channelStreamTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>();
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    chan.stream().consume((i) -> res.set(i));

    chan.tell(1);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(1));
  }

  @Test
  public void drainedChannelTest() throws InterruptedException {
    AVar<Integer> res = new AVar<>();
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    chan.stream().consume((i) -> res.set(i));

    chan.tell(1);

    assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(1));
    Exception expectedException = null;
    try {
      chan.get();
    } catch (Exception e) {
      expectedException = e;
    }
    assertTrue(expectedException != null);
  }

  @Test
  public void consumingPublishingChannelsTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    Subscriber<Integer> subscriber = chan.subscriber();
    CountDownLatch requestLatch = new CountDownLatch(2);
    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(long l) {
        requestLatch.countDown();
      }

      @Override
      public void cancel() {

      }
    });
    subscriber.onNext(1);
    subscriber.onNext(2);

    assertThat(chan.get(10, TimeUnit.SECONDS), is(1));
    assertThat(chan.get(10, TimeUnit.SECONDS), is(2));
    assertTrue(chan.get() == null);

    chan.tell(3);
    assertThat(chan.get(10, TimeUnit.SECONDS), is(3));

    requestLatch.await();
    assertThat(requestLatch.getCount(), is(0L));
  }

  @Test
  public void timedGetTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      try {
        Thread.sleep(1000);
        chan.tell(1);
        latch.countDown();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();

    latch.await(LATCH_TIMEOUT, LATCH_TIME_UNIT);
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(1L));
    assertThat(chan.get(2000, TimeUnit.MILLISECONDS), is(1));
  }

  @Test
  public void timedGetUnresolvedTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    Channel<Integer> chan = pipe.channel();

    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(1L));
    boolean caught = false;
    try {
      System.out.println(chan.get(100, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      caught = true;
    }
    assertThat(caught, is(true));
    chan.dispose();
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(0L));
  }

  @Test
  public void channelDisposeTest() throws InterruptedException {
    NamedPipe<Integer> pipe = new NamedPipe<>();
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(0L));
    Channel<Integer> chan = pipe.channel();
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(1L));
    chan.dispose();
    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(0L));
  }

}
