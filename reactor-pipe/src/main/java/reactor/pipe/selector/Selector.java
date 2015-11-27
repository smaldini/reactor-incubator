package reactor.pipe.selector;

import reactor.fn.Predicate;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public interface Selector<T> extends Predicate<T> {
  default Function<T, Map<String, Object>> getHeaderResolver() {
    return (i) -> Collections.emptyMap();
  }
}
