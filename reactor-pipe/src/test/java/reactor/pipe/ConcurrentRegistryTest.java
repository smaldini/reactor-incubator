package reactor.pipe;

import org.junit.Test;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.registry.Registration;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ConcurrentRegistryTest {

  @Test
  public void multipleSubscriptionsTest() throws InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ConcurrentRegistry<Key> registry = new ConcurrentRegistry<>();
    registry.addKeyMissMatcher((key) -> key.getPart(0).equals("part"),
                               key -> Collections.singletonMap(key,
                                                               (key1, value) -> latch1.countDown()));

    registry.addKeyMissMatcher((key) -> key.getPart(0).equals("part"),
                               key -> Collections.singletonMap(key,
                                                               (key1, value) -> latch2.countDown()));

    Key key = Key.wrap("part");

    registry.select(key).forEach(new Consumer<Registration<Key>>() {
      @Override
      public void accept(Registration<Key> keyRegistration) {
        keyRegistration.getObject().accept(key,
                                           1);
      }
    });

    latch1.await(10, TimeUnit.SECONDS);
    latch2.await(10, TimeUnit.SECONDS);

    assertThat(latch1.getCount(), is(0L));
    assertThat(latch2.getCount(), is(0L));
  }
}
