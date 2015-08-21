package reactor.pipe.example.twitter.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FollowEvent extends Event {

  private final String follower;
  private final String followed;

  @JsonCreator
  public FollowEvent(@JsonProperty("follower") String follower,
                     @JsonProperty("followed") String followed) {
    this.follower = follower;
    this.followed = followed;
  }

}
