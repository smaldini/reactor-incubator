package reactor.kafka.support;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class KeySerializer implements Serializer<Key> {

  private final StringSerializer stringSerializer;

  public KeySerializer() {
    this.stringSerializer = new StringSerializer();
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    stringSerializer.configure(map, b);
  }

  @Override
  public byte[] serialize(String topic, Key key) {
    return stringSerializer.serialize(topic,
                                      String.join(",",
                                                  Arrays.stream(key.getObjects())
                                                        .map(Object::toString)
                                                        .collect(Collectors.toList())));
  }

  @Override
  public void close() {
  }
}
