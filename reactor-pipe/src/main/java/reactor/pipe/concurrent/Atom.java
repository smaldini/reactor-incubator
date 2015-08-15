package reactor.pipe.concurrent;

import reactor.fn.tuple.Tuple2;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.UnaryOperator;

/**
 * Generic Atom
 */
public class Atom<T> {

  private AtomicReference<T> ref;

  public Atom(T ref) {
    this.ref = new AtomicReference<T>(ref);
  }

  public T deref() {
    return ref.get();
  }

  public T update(UnaryOperator<T> swapOp) {
    for (; ; ) {
      T old = ref.get();
      T newv = swapOp.apply(old);
      if (ref.compareAndSet(old, newv)) {
        return newv;
      }
    }
  }

  public T updateAndReturnOld(UnaryOperator<T> swapOp) {
    for (; ; ) {
      T old = ref.get();
      T newv = swapOp.apply(old);
      if (ref.compareAndSet(old, newv)) {
        return old;
      }
    }
  }

  public <O> O updateAndReturnOther(Function<T, Tuple2<T, O>> swapOp) {
    for (; ; ) {
      T old = ref.get();
      Tuple2<T, O> newvtuple = swapOp.apply(old);
      if (ref.compareAndSet(old, newvtuple.getT1())) {
        return newvtuple.getT2();
      }
      LockSupport.parkNanos(1L);
    }
  }

  public <O> O updateAndReturnOther(Predicate<T> pred,
                                    Function<T, Tuple2<T, O>> swapOp) {
    for (; ; ) {
      T old = ref.get();
      if (pred.test(old)) {
        Tuple2<T, O> newvtuple = swapOp.apply(old);
        if (ref.compareAndSet(old, newvtuple.getT1())) {
          return newvtuple.getT2();
        }
        LockSupport.parkNanos(1L); //TODO: Maybe park everywhere?
      }
    }
  }

  public T reset(T newv) {
    for (; ; ) {
      T old = ref.get();
      if (ref.compareAndSet(old, newv)) {
        return newv;
      }
    }
  }


}
