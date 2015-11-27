package reactor.pipe.selector;

import reactor.fn.Function;
import reactor.fn.Predicate;

import java.util.Collections;
import java.util.Map;

public interface Selector<T> extends Predicate<T> {
  default Function<T, Map<String, Object>> getHeaderResolver() {
    return (i) -> Collections.emptyMap();
  }
}
