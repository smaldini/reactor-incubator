package reactor.pipe;

import org.junit.Test;
import reactor.pipe.consumer.KeyedConsumer;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.registry.Registration;
import reactor.pipe.registry.Registry;
import reactor.pipe.selector.Selector;
import reactor.pipe.selector.Selectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ConcurrentRegistryTest {

  @Test
  public void multipleSubscriptionsTest() throws InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ConcurrentRegistry<Key> registry = new ConcurrentRegistry<>();
    registry.register((key) -> key.getPart(0).equals("part"),
                               key -> Collections.singletonMap(key,
                                                               (key1, value) -> latch1.countDown()));

    registry.register((key) -> key.getPart(0).equals("part"),
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

  @Test
  public void registrationsWithTheSameSelectorAreOrderedByInsertionOrder() {
    final Registry<Object> registry = new ConcurrentRegistry<>();

    String key = "selector";
    KeyedConsumer consumer1 = (key1, value) -> {};
    KeyedConsumer consumer2 = (key1, value) -> {};
    KeyedConsumer consumer3 = (key1, value) -> {};
    KeyedConsumer consumer4 = (key1, value) -> {};
    KeyedConsumer consumer5 = (key1, value) -> {};

    registry.register(key, consumer1);
    registry.register(key, consumer2);
    registry.register(key, consumer3);
    registry.register(key, consumer4);
    registry.register(key, consumer5);

    Iterable<Registration<Object>> registrations = registry.select(key);

    List<Object> objects = new ArrayList<Object>();
    for (Registration<?> registration : registrations) {
      if (null != registration) {
        objects.add(registration.getObject());
      }
    }

    assertEquals(Arrays.asList(consumer1,
                               consumer2,
                               consumer3,
                               consumer4,
                               consumer5),
                 objects);
  }
}
