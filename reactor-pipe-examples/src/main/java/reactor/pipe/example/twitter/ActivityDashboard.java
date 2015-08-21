package reactor.pipe.example.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple2;
import reactor.kafka.KafkaSubscriber;
import reactor.pipe.Firehose;
import reactor.pipe.key.Key;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ActivityDashboard {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActivityDashboard.class);


  private ObjectMapper objectMapper;

  public ActivityDashboard() {
    this.objectMapper = new ObjectMapper();

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    executorService.submit(producerRunnable());
    executorService.submit(consumerRunnable());
  }

  protected Runnable producerRunnable() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty("bootstrap.servers", "workstation:9092");
    producerProperties.setProperty("key.serializer", "reactor.pipe.example.twitter.KeySerializer");
    producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new Runnable() {
      @Override
      public void run() {
        try {
          KafkaSubscriber<Key, String> subscriber = new KafkaSubscriber<Key, String>(producerProperties,
                                                                                     "events",
                                                                                     k -> 0,
                                                                                     new Consumer<Throwable>() {
                                                                                       @Override
                                                                                       public void accept(
                                                                                         Throwable throwable) {
                                                                                         LOGGER.error(
                                                                                           "Received an error",
                                                                                           throwable);
                                                                                       }
                                                                                     });

          Firehose<Key> firehose = new Firehose();
          Publisher<Tuple2<Key, String>> publisher = firehose.makePublisher(Key.wrap("kafka"));

          publisher.subscribe(subscriber);

          for (; ; ) {
            LOGGER.info("Ping sent...");
            firehose.notify(Key.wrap("kafka"),
                            "ping");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        } catch (Exception e) {
          LOGGER.error("", e);
        }
      }
    };
  }


  protected Runnable consumerRunnable() {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("bootstrap.servers", "workstation:9092");
    consumerProperties.setProperty("zookeeper.connect", "workstation:2181");
    consumerProperties.setProperty("group.id", "testGroup");
    consumerProperties.setProperty("auto.offset.reset", "largest");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("auto.commit.interval.ms", "1000");
    consumerProperties.setProperty("session.timeout.ms", "1000");

    return new Runnable() {
      @Override
      public void run() {

      }
    };
  }


  public static void main(String[] args) {
    ActivityDashboard dashboard = new ActivityDashboard();
  }

}
