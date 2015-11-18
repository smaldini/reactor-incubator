package reactor.pipe.key;

public class TopicKey extends Key {

  public TopicKey(String topic) {
    super(new String[]{topic});
  }

  public Key derive() {
    throw new RuntimeException("Can't derive a topic key");
  }

  public Key clone(Key metadataSource) {
    throw new RuntimeException("Can't clone a topic key");
  }

  @Override
  public Key clone() {
    throw new RuntimeException("Can't clone a topic key");
  }

}
