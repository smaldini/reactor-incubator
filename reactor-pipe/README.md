# Reactor Pipes - in-process channels and streams

Reactor Pipes is foundation for building complex, performant distributed and in-memory
stream processing toplogies. 

Main features include:

  * [Named Pipe topologies](https://github.com/reactor/reactor-extensions/tree/master/reactor-pipe#named-topologies)
  * [Anonymous Pipe topologies](https://github.com/reactor/reactor-extensions/tree/master/reactor-pipe#anonymous-topologies)
  * [Matched Lazy Pipes](https://github.com/reactor/reactor-extensions/tree/master/reactor-pipe#matched-lazy-topologies)
  * [Uni- and Bi- directional Channels](https://github.com/reactor/reactor-extensions/blob/master/reactor-pipe/README.md#uni--and-bi--directional-channels)
  * Independent Per-Entity Pipe
  * Atomic State operations
  * Persistent-collection based handlers
  * Multiple dispatch strategies
  * Small and lightweight dependency
  * Well documented

Near-future plans for extensions:

  * Integration with Databases for persisting Stream information between restarts
  * Integration with Message Queues for distributed and fault-tolerant processing
  * Mesos integration for distribution
  
## Why Pipes?

Current state-of-art Stream Processing solutions bring enormous
development and maintenance overhead and can mostly serve as a
replacement for Extract-Load-Transform / Batch Processing jobs (think
Hadoop).

Reactor Pipes is a lightweight Stream Processing system, which was
produced with performance, composability, development and support
simplicity in mind. You can scale it on one or many machines without any
trouble, building complex processing topologies that are as easy to test
as normal operations on `Collections`. After all, Streams are nothing
but eternal sequences.

Reactor Pipes can be also used to make asynchronous/concurrent programs
simpler, easier to maintain and construct. For example, you could use
__Channels__ with __Matched Pipes__ in order to build a lightweight
websocket server implementation. Another example is asynchronous
handlers in your HTTP server, which would work with any driver, no
matter whether it offers asynchronous API or no. Other examples include
IoT, Machine Learning, Business Analytics and more.

The problem with Machine Learning on Streams is that most of algorithms
assume some kind of state: for example, Classification assumes that
trained model is available. Having this state somewhere in the Database
or Cache brings additional deserialization overhead, and having it in
memory might be hard if the system doesn't give you partitioning
guarantees (that requests dedicated to same logical entity will end up
on the same node).

Reactor Pipes approach is simple: develop on one box, for one box, break
processing in logical steps for distribution and scale up upon the
need. Because of the nature and the layout of wiring between pipes and
data flows, you will be able to effortlessly scale it up.

## Terminology

`Stream` is a term coming from Reactive Programming. Stream looks a little
like a collection from the consumer perspective, with the only
difference that if collection is a ready set of events, stream is an
infinite collection. If you do `map` operation on the stream, `map`
function will see each and every element coming to the stream.

`Publisher` (`generator` or `producer` in some terminologies) is a
function or entity that publishes items to the stream. `Consumer` (or
`listener`, in some terminologies) is a function that is subscribed to
the stream, and will be asyncronously getting items that the `publisher`
publishes to the stream.

In some cases, pipes can serve simultaneously as a `consumer` and as a
`producer`. For example, `map` is consumed to the events coming in
stream, and publishes modified events further downstream.

`Topology` is a stream with a chain of publishers and producers attached
to it. For example, you can have a stream that `maps` items,
incrementing each one of them, then a `filter` that picks up only even
incremented numbers. Of course, in real life applications topologies are
much more complex.

`Source` / `Sink` are used to describe the order of functions in your
topologies.  For example, if you have a stream that `maps` items,
incrementing each one of them, it serves as an upstream for the
following `filter` function, that consumes events from it.  `Filter`
serves as a `downstream` in this example.

`Named` and `anonymous` pipes are just the means of explaning the
wiring between the parts of the topology. If each item within the
`named` stream has to know where to subscribe and where to publish the
resulting events, in `anonymous` topologies the connection between
stream parts is implicit, e.g. parts are simply wired by the systems
with unique randomly generated keys.

And a little more description about each one of them:

## Named Topologies

These are the "traditional" key/value subscriptions": you can subscribe
to any named Stream.  As soon as the message with a certain Key is
coming to the stream, it will be matched and passed to all subscribed
handlers.

```java
Pipe<Integer> intPipe = new Pipe<>();

intPipe.map(Key.wrap("key1"), // subscribe to
            Key.wrap("key2"), // downstream result to
            (i) -> i + 1);    // mapper function
              
intPipe.consume(Key.wrap("key2"), // subscribe to result of mapper 
                (i) -> System.out.println(i));

// send a couple of payloads
intPipe.notify(Key.wrap("key1"), 1);
// => 2
intPipe.notify(Key.wrap("key1"), 1);
// => 3
```

## Anonymous Topologies

Anonymous streams are chain of decoupled async stream operations that
represent a single logical operation. Anonymous Topology is subscribed
to the particular key, and will create all the additional wiring between
handlers in the Anonymous Topologies automatically.

```java
Pipe<Integer> pipe = new Pipe<>();
AVar<Integer> res = new AVar<>();

pipe.anonymous(Key.wrap("source"))        // create an anonymous topology subscribed to "source"
    .map(i -> i + 1)                      // add add 1 to each incoming value
    .map(i -> i * 2)                      // multiply all incoming values by 2
    .consume(i -> System.out.println(i)); // output every incoming value to stdout

pipe.notify(Key.wrap("source"), 1); 
// => 2

pipe.notify(Key.wrap("source"), 2);
// => 4
```

The main difference between Java Streams and Reactor Pipes is the
dispatch flexibility and combination of multiple streaming
paradigms. You can pick the underlaying dispatcher depending on whether
your processing pipeline consists of short or long lived functions and
so on.

## Matched Lazy Topologies

If you want to have flexible subscriptions (for example, subscribe
to all the keys that match some predicate), normally you'd have to
check each matcher against the key. The more matchers you have,
the slower matching gets.

However, with Matched Lazy Topologies, your lookups will still complete
in constant time (`O(1)`) on average. In order to allow such a
flexibility, you specify the predicate that will check if the key
should be processed by this topology. Whenever first key comes,
all matchers will be queried and topologies will get created for
the matched keys. All subsequent messages with same key will be
processed immediately. 

This is not just a performance optimisation, this also allows you
to process different entities independently. 

```java
pipe.matched(key -> key.getPart(0).equals("source")) // create an anonymous stream subscribed to "source"
    .map(i -> i + 1)                                 // add add 1 to each incoming value
    .map(i -> i * 2)                                 // multiply all incoming values by 2
    .consume(i -> System.out.println(i));            // output every incoming value to stdout

firehose.notify(Key.wrap("source", "first"), 1);
// => 2

firehose.notify(Key.wrap("source", "second"), 2);
// => 4
```

As you can see, toplogies will be created per-entity, which opens op a
lot of opportunities for independent stream processing, entity matching
and storing per-entity state.

## Independent Per-Entity Toplogies

No more need to manage streams for multiple logical entities in the same handler,
we'll do that for you. This future is used together with Matched Lazy Streams, so
every entity stream will have it's own in-memory state.

## Atomic State operations

Since entity streams are independent and locks are expensive, it's important to
keep the operations lock-free. For that you're provided with an `Atom<T>` which
will ensure lock-free atomic updates to the state.

And since the state for entity are split, you're able to save and restore between
restarts of your processing topologies.

```java
Pipe<Integer> intPipe = new Pipe<>(firehose);

intPipe.map(Key.wrap("key1"), // source
            Key.wrap("key2"), // destination 
            (i) -> i + 1);    // mapping function
            
intPipe.map(Key.wrap("key2"),
            Key.wrap("key3"),
            (Atom<Integer> state) -> {                 // Use a supplier to capture state in closure 
                return (i) -> {                        // Return a function, just as a "regular" map would do
                  return state.swap(old -> old + i);   // Access internal state
                };
              },
              0);                                      // Pass the initial value for state
intPipe.consume(Key.wrap("key3"), ());

intPipe.notify(Key.wrap("key1"), 1);
// => 2
intPipe.notify(Key.wrap("key1"), 2);
// => 5 
intPipe.notify(Key.wrap("key1"), 3);
// => 9
```
## Uni- and Bi- directional Channels

Channels are much like a queue you can publish to and pull your changes
from the queue-like object. This feature is particularly useful in
scenarios when you don't need to have neither subscription nor publish
hey, and you need only to have async or sync uni- or bi- directional
communication.

```java
Pipe<Integer> intPipe = new Pipe<>(firehose);
Channel<Integer> chan = intPipe.channel();

chan.tell(1);
chan.tell(2);

chan.get();
// => 1

chan.get();
// => 2

chan.get();
// => null
```

Channels can be consumed by the pipes, too. It is although important to
remember that in order to avoid unnecessary state accumulation Channels
can either be used as a pipe or as a channel:

```java
Pipe<Integer> pipe = new Pipe<>(firehose);
Channel<Integer> chan = pipe.channel();

chan.stream()
    .map(i -> i + 1)
    .consume(i -> System.out.println(i));

chan.tell(1);
// => 2

chan.get();
// throws RuntimeException, since channel is already drained by the stream
```

Channels can also be split to publishing and consuming channels for type
safety, if you need to ensure that consuming part can't publish messages
and publishing part can't accidentally consume them:

```java
Pipe<Integer> pipe = new Pipe<>(firehose);
Channel<Integer> chan = pipe.channel();

PublishingChannel<Integer> publishingChannel = chan.publishingChannel();
ConsumingChannel<Integer> consumingChannel = chan.consumingChannel();

publishingChannel.tell(1);
publishingChannel.tell(2);

consumingChannel.get();
// => 1

consumingChannel.get();
// => 2
```
# License

Copyright © 2014 Alex Petrov 

Reactor Pipes is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
