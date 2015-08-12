package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

public interface StateProvider {

  public <SRC, T> Atom<T> makeAtom(SRC src, T init);

}
