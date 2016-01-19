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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQChannel implements ReactiveChannel<Buffer, Buffer>, Publisher<Buffer> {

	private final ZeroMQConsumerSpec eventSpec = new ZeroMQConsumerSpec();
	private final InetSocketAddress remoteAddress;

	private volatile String     connectionId;
	private volatile ZMQ.Socket socket;

	Subscriber<? super Buffer> inputSub;

	public ZeroMQChannel(InetSocketAddress remoteAddress) {
		this.remoteAddress = remoteAddress;
	}

	@Override
	public Mono<Void> writeBufferWith(Publisher<? extends Buffer> dataStream) {
		return writeWith(dataStream);
	}

	@Override
	public Publisher<Buffer> input() {
		return this;
	}

	@Override
	public Mono<Void> writeWith(Publisher<? extends Buffer> writer) {
		return new Mono<Void>() {
			@Override
			public void subscribe(final Subscriber<? super Void> postWriter) {

				writer.subscribe(new BaseSubscriber<Buffer>() {

					ZMsg currentMsg;

					@Override
					public void onSubscribe(final Subscription subscription) {
						eventSpec.close(new Runnable() {
							@Override
							public void run() {
								subscription.cancel();
							}
						});
						subscription.request(Long.MAX_VALUE);
						postWriter.onSubscribe(EmptySubscription.INSTANCE);
					}

					@Override
					public void onNext(Buffer out) {
						super.onNext(out);
						final ByteBuffer data = out.byteBuffer();

						byte[] bytes = new byte[data.remaining()];
						data.get(bytes);
						boolean isNewMsg;
						ZMsg msg;
						msg = currentMsg;
						currentMsg = new ZMsg();
						if (msg == null) {
							msg = currentMsg;
							isNewMsg = true;
						}
						else {
							isNewMsg = false;
						}

						if (isNewMsg) {
							switch (socket.getType()) {
								case ZMQ.ROUTER:
									msg.add(new ZFrame(connectionId));
									break;
								default:
							}
						}
						msg.add(new ZFrame(bytes));
					}

					@Override
					public void onError(Throwable throwable) {
						super.onError(throwable);
						doFlush(currentMsg, null);
						postWriter.onError(throwable);
					}

					@Override
					public void onComplete() {
						doFlush(currentMsg, postWriter);
					}
				});
			}
		};
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		if (subscriber == null) {
			throw new IllegalStateException("Input Subscriber cannot be null");
		}
		synchronized (this) {
			if (inputSub != null) {
				return;
			}
			inputSub = subscriber;
		}

		inputSub.onSubscribe(EmptySubscription.INSTANCE);
	}

	public ZeroMQChannel setConnectionId(String connectionId) {
		this.connectionId = connectionId;
		return this;
	}

	public ZeroMQChannel setSocket(ZMQ.Socket socket) {
		this.socket = socket;
		return this;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return remoteAddress;
	}

	public void close() {
		try {
			final List<Runnable> closeHandlers;
			synchronized (eventSpec.closeHandlers) {
				closeHandlers = new ArrayList<Runnable>(eventSpec.closeHandlers);
			}

			for (Runnable r : closeHandlers) {
				r.run();
			}
		}
		catch (Throwable t) {
			if (inputSub != null) {
				inputSub.onError(t);
			}
		}
	}

	private void doFlush(ZMsg msg, final Subscriber<? super Void> onComplete) {
		if (null != msg) {
			boolean success = msg.send(socket);
			if (null != onComplete) {
				if (success) {
					onComplete.onComplete();
				}
				else {
					onComplete.onError(new RuntimeException("ZeroMQ Message could not be sent"));
				}
			}
		}
	}

	@Override
	public ConsumerSpec on() {
		return eventSpec;
	}

	@Override
	public ZMQ.Socket delegate() {
		return socket;
	}

	@Override
	public String toString() {
		return "ZeroMQNetChannel{" +
				"closeHandlers=" + eventSpec.closeHandlers +
				", connectionId='" + connectionId + '\'' +
				", socket=" + socket +
				'}';
	}

	private static class ZeroMQConsumerSpec implements ConsumerSpec {

		final List<Runnable> closeHandlers = new ArrayList<>();

		@Override
		public ConsumerSpec close(Runnable onClose) {
			synchronized (closeHandlers) {
				closeHandlers.add(onClose);
			}
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, Runnable onReadIdle) {
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, Runnable onWriteIdle) {
			return this;
		}
	}

}
