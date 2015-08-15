package reactor.pipe.redis;

import reactor.pipe.concurrent.Atom;
import reactor.pipe.state.StateProvider;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RedisStateProvider implements StateProvider {

  @Override
  public <SRC, T> Atom<T> makeAtom(SRC src, T init) {
    throw new NotImplementedException();
  }

}
