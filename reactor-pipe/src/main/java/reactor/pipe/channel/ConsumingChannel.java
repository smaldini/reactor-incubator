package reactor.pipe.channel;

import reactor.pipe.AnonymousFlow;

import java.util.concurrent.TimeUnit;

public interface ConsumingChannel<T> {

  public T get();
  public T get(long time, TimeUnit timeUnit) throws InterruptedException;
  public AnonymousFlow<T> stream();

}
