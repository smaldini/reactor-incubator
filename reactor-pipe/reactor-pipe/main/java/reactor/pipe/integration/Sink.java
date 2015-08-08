package reactor.pipe.integration;

import reactor.pipe.Firehose;
import reactor.pipe.key.Key;

public abstract class Sink<K extends Key, V> {

  private final Firehose firehose;

  public Sink(Firehose firehose) {
    this.firehose = firehose;
  }

  public void publish(PipeTuple<K, V> pipeTuple) {
    this.firehose.notify(pipeTuple.getKey(), pipeTuple.getValue());
  }
}
