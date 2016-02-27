/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.net.impl.zmq.tcp;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.core.timer.Timer;
import reactor.core.util.Assert;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.io.buffer.Buffer;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.Spec;
import reactor.io.net.impl.zmq.ZeroMQClientSocketOptions;
import reactor.io.net.impl.zmq.ZeroMQServerSocketOptions;
import reactor.rx.Promise;
import reactor.rx.Streams;
import reactor.rx.net.ChannelStream;
import reactor.rx.net.NetStreams;
import reactor.rx.net.ReactorChannelHandler;
import reactor.rx.net.tcp.ReactorTcpClient;
import reactor.rx.net.tcp.ReactorTcpServer;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQ<T> {

	private final static Map<Integer, String> SOCKET_TYPES = new HashMap<>();

	static {
		for (Field f : ZMQ.class.getDeclaredFields()) {
			if (int.class.isAssignableFrom(f.getType())) {
				f.setAccessible(true);
				try {
					int val = f.getInt(null);
					SOCKET_TYPES.put(val, f.getName());
				}
				catch (IllegalAccessException e) {
				}
			}
		}
	}

	private final Timer    timer;
	private final ZContext zmqCtx;
	private final Preprocessor<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>, T, T, ReactiveChannel<T, T>>
	                       preprocessor;

	private final List<ReactivePeer<T, T, ReactiveChannel<T, T>>> peers = new ArrayList<>();

	private volatile boolean shutdown = false;

	public ZeroMQ(Timer env) {
		this(env, null);
	}

	public ZeroMQ(Timer env,
			Preprocessor<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>, T, T, ReactiveChannel<T, T>> preprocessor) {
		this.timer = env;
		this.zmqCtx = new ZContext();
		this.preprocessor = preprocessor;
		this.zmqCtx.setLinger(100);
	}

	public static String findSocketTypeName(final int socketType) {

		String type = SOCKET_TYPES.get(socketType);
		if (type == null || type.isEmpty()) {
			return "";
		}
		else {
			return type;
		}

	}

	public Promise<ChannelStream<T, T>> dealer(String addrs) {
		return createClient(addrs, ZMQ.DEALER);
	}

	public Promise<ChannelStream<T, T>> push(String addrs) {
		return createClient(addrs, ZMQ.PUSH);
	}

	public Promise<ChannelStream<T, T>> pull(String addrs) {
		return createServer(addrs, ZMQ.PULL);
	}

	public Promise<ChannelStream<T, T>> request(String addrs) {
		return createClient(addrs, ZMQ.REQ);
	}

	public Promise<ChannelStream<T, T>> reply(String addrs) {
		return createServer(addrs, ZMQ.REP);
	}

	public Promise<ChannelStream<T, T>> router(String addrs) {
		return createServer(addrs, ZMQ.ROUTER);
	}

	public Promise<ChannelStream<T, T>> createClient(final String addrs,
			final int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");

		ReactorTcpClient<T, T> client =
				NetStreams.tcpClient(ZeroMQTcpClient.class, new ReactiveNet.TcpClientFactory<T, T>() {

					@Override
					public Spec.TcpClientSpec<T, T> apply(Spec.TcpClientSpec<T, T> spec) {
						return spec.timer(timer)
						           .preprocessor(preprocessor)
						           .options(new ZeroMQClientSocketOptions().context(zmqCtx)
						                                                   .connectAddresses(addrs)
						                                                   .socketType(socketType));
					}
				});

		final Promise<ChannelStream<T, T>> promise = Promise.ready(timer);
		client.start(new ReactorChannelHandler<T, T>() {
			@Override
			public Publisher<Void> apply(ChannelStream<T, T> ttChannelStream) {
				promise.onNext(ttChannelStream);
				return Streams.never();
			}
		});

		synchronized (peers) {
			peers.add(client.delegate());
		}

		return promise;
	}

	public Promise<ChannelStream<T, T>> createServer(final String addrs,
			final int socketType) {
		Assert.isTrue(!shutdown, "This ZeroMQ instance has been shut down");

		final ReactorTcpServer<T, T> server =
				NetStreams.tcpServer(ZeroMQTcpServer.class, new ReactiveNet.TcpServerFactory<T, T>() {

					@Override
					public Spec.TcpServerSpec<T, T> apply(Spec.TcpServerSpec<T, T> spec) {
						return spec.timer(timer)
						           .preprocessor(preprocessor)
						           .options(new ZeroMQServerSocketOptions().context(zmqCtx)
						                                                   .listenAddresses(addrs)
						                                                   .socketType(socketType));
					}
				});

		final Promise<ChannelStream<T, T>> promise = Promise.ready(timer);
		server.start(new ReactorChannelHandler<T, T>() {
			@Override
			public Publisher<Void> apply(ChannelStream<T, T> ttChannelStream) {
				promise.onNext(ttChannelStream);
				return Streams.never();
			}
		});

		synchronized (peers) {
			peers.add(server.delegate());
		}

		return promise;
	}

	public void shutdown() {
		if (shutdown) {
			return;
		}
		shutdown = true;

		List<ReactivePeer<T, T, ReactiveChannel<T, T>>> _peers;
		synchronized (peers) {
			_peers = new ArrayList<>(peers);
		}

		Streams.fromIterable(_peers)
		       .flatMap(new Function<ReactivePeer, Publisher<Void>>() {
			       @Override
			       @SuppressWarnings("unchecked")
			       public Publisher<Void> apply(
					       final ReactivePeer ttChannelStreamReactorPeer) {
				       return ttChannelStreamReactorPeer.shutdown()
				                      .doOnSuccess(new Consumer() {
					                      @Override
					                      public void accept(Object o) {
						                      peers.remove(ttChannelStreamReactorPeer);
					                      }
				                      });
			       }
		       })
		       .consume();

//		if (log.isDebugEnabled()) {
//			log.debug("Destroying {} on {}", zmqCtx, this);
//		}
//		zmqCtx.destroy();
	}

}
