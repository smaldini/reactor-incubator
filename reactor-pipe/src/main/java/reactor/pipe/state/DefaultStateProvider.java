package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

public class DefaultStateProvider implements StateProvider {

  @Override
  public <SRC, T> Atom<T> makeAtom(SRC src, T init) {
    return new Atom<>(init);
  }
}
