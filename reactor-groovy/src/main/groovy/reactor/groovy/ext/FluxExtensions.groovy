/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.groovy.ext

import groovy.transform.CompileStatic
import org.reactivestreams.Processor
import org.reactivestreams.Subscriber
import reactor.Cancellation
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate

//import reactor.io.codec.Codec
//import reactor.rx.IOStreams
/**
 * Glue for Groovy closures and operator overloading applied to Stream, Composable,
 * Promise and Deferred.
 * Also gives convenient Deferred handling by providing map/filter/consume operations
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class FluxExtensions {

  /**
   * Alias
   */

  // IO Streams
  /*static <SRC, IN> Flux<IN> decode(final Publisher<? extends SRC> publisher, Codec<SRC, IN, ?> codec) {
    IOStreams.decode(codec, publisher)
  }*/

  /**
   * Operator overloading
   */
  static <T> Mono<T> mod(final Flux<T> selfType, final BiFunction<T, T, T> other) {
    selfType.reduce other
  }

  //Mapping
  static <O, E extends Subscriber<? super O>> E or(final Flux<O> selfType, final E other) {
    selfType.subscribeWith(other)
  }

  static <T, V> Flux<V> or(final Flux<T> selfType, final Function<T, V> other) {
    selfType.map other
  }

  static <T, V> Mono<V> or(final Mono<T> selfType, final Function<T, V> other) {
    selfType.map(other)
  }

  //Filtering
  static <T> Flux<T> and(final Flux<T> selfType, final Predicate<T> other) {
    selfType.filter other
  }

  static <T> Mono<T> and(final Mono<T> selfType, final Predicate<T> other) {
    selfType.filter(other)
  }

  //Consuming
  static <T> Cancellation leftShift(final Flux<T> selfType, final Consumer<T> other) {
    selfType.subscribe other
  }

  static <T> Mono<T> leftShift(final Mono<T> selfType, final Consumer<T> other) {
    selfType.doOnSuccess other
  }

  static <T> List<T> rightShift(final Flux<T> selfType, final List<T> other) {
    selfType.collect ({ other },  { List<T> a, b -> a.add(b)}).block()
  }

  //Consuming
  static <T, P extends Processor<?, T>> P leftShift(final P selfType, final T value) {
    selfType.onNext(value)
    selfType
  }

}