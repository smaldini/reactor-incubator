package reactor.pipe.codec;

import reactor.fn.Function;

public class Codec<IN, OUT> {

  private final Function<IN, OUT> encoder;
  private final Function<OUT, IN> decoder;

  public Codec(Function<IN, OUT> encoder,
               Function<OUT, IN> decoder) {
    this.encoder = encoder;
    this.decoder = decoder;
  }

  public OUT encode(IN toEncode) {
    return this.encoder.apply(toEncode);
  }

  public IN decode(OUT toDecode) {
    return this.decoder.apply(toDecode);
  }

}


