/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.io.net.impl.zmq;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.Timers;
import reactor.core.support.UUIDUtils;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JacksonJsonCodec;
import reactor.io.codec.kryo.KryoCodec;
import reactor.io.net.NetStreams;
import reactor.io.net.impl.zmq.tcp.ZeroMQ;
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpClient;
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpServer;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ZeroMQClientServerTests extends AbstractNetClientServerTest {

	static Kryo                  KRYO;
	static KryoCodec<Data, Data> KRYO_CODEC;
	static ZeroMQ<Data>          ZEROMQ;

	CountDownLatch  latch;
	ExecutorService threadPool;

	final int msgs    = 10;
	final int threads = 4;

	@BeforeClass
	public static void classSetup() {
		KRYO = new Kryo();
		KRYO_CODEC = new KryoCodec<>(KRYO, false);
		ZEROMQ = new ZeroMQ<Data>(Timers.global()).codec(KRYO_CODEC);
	}

	@AfterClass
	public static void classCleanup() {
		ZEROMQ.shutdown();
		//ENV.shutdown();
	}


	@Before
	public void loadEnv() {
		Timers.global();
		threadPool = Executors.newCachedThreadPool();
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
		Timers.unregisterGlobal();
	}

	@Override
	public void setup() {
		super.setup();
		latch = new CountDownLatch(1);
	}

	@Test(timeout = 60000)
	public void clientSendsDataToServerUsingKryo() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class, ZeroMQTcpClient.class, KRYO_CODEC, data, d -> d.equals(data));
	}

	@Test(timeout = 60000)
	public void clientSendsDataToServerUsingJson() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class, ZeroMQTcpClient.class, new JacksonJsonCodec<>(), data, d -> d.equals(data));
	}

	@Test(timeout = 60000)
	public void clientSendsDataToServerUsingBuffers() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class, ZeroMQTcpClient.class, Buffer.wrap("Hello World!"));
	}

	@Test//(timeout = 60000)
	public void zmqRequestReply() throws InterruptedException {
		ZEROMQ.reply("tcp://*:" + getPort())
		      .onSuccess(ch -> ch.writeWith(ch.observe(d -> latch.countDown()))
		                         .consume());

		ZEROMQ.request("tcp://127.0.0.1:" + getPort())
		      .onSuccess(ch -> {
			      ch.consume(d -> latch.countDown());
			      ch.writeWith(Streams.just(data))
			        .consume();
		      });

		assertTrue("REQ/REP socket exchanged data", latch.await(5, TimeUnit.SECONDS));
	}

	@Test(timeout = 60000)
	public void zmqPushPull() throws InterruptedException {
		ZEROMQ.pull("tcp://*:" + getPort())
		      .onSuccess(ch -> latch.countDown());

		ZEROMQ.push("tcp://127.0.0.1:" + getPort())
		      .onSuccess(ch -> ch.writeWith(Streams.just(data))
		                         .consume());

		assertTrue("PULL socket received data", latch.await(1, TimeUnit.SECONDS));
	}

	@Test(timeout = 60000)
	public void zmqRouterDealer() throws InterruptedException {
		ZEROMQ.router("tcp://*:" + getPort())
		  .onSuccess(ch -> latch.countDown());

		ZEROMQ.dealer("tcp://127.0.0.1:" + getPort())
		  .onSuccess(ch ->
			  ch.writeWith(Streams.just(data).log("zmqp")).consume()
		  );

		assertTrue("ROUTER socket received data", latch.await(50, TimeUnit.SECONDS));
	}

	@Test(timeout = 60000)
	public void zmqInprocRouterDealer() throws InterruptedException {
		ZEROMQ.router("inproc://queue" + getPort())
		  .onSuccess(ch -> {
			  ch.consume(data -> {
				  latch.countDown();
			  });
		  });

		// we have to sleep a couple cycles to let ZeroMQ get set up on inproc
		Thread.sleep(500);

		ZEROMQ.dealer("inproc://queue" + getPort())
		  .onSuccess(ch -> ch.writeWith(Streams.just(data)).consume());

		assertTrue("ROUTER socket received inproc data", latch.await(5, TimeUnit.SECONDS));
	}

	@Test(timeout = 60000)
	@Ignore
	public void exposesZeroMQServer() throws InterruptedException {
		final int port = SocketUtils.findAvailableTcpPort();
		final CountDownLatch latch = new CountDownLatch(2);
		ZContext zmq = new ZContext();

		TcpServer<Buffer, Buffer> server = NetStreams.tcpServer(ZeroMQTcpServer.class, spec -> spec
						.listen("127.0.0.1", port)
		);

		server.start(ch -> ch.writeWith(ch.take(1)
		                                  .observe(buff -> {
			                                  if (buff.remaining() == 128) {
				                                  latch.countDown();
			                                  }
			                                  else {
				                                  log.info("data: {}", buff.asString());
			                                  }
		                                  })
		                                  .map(d -> Buffer.wrap("Goodbye World!"))
		                                  .log("conn"))).await();

		ZeroMQWriter zmqw = new ZeroMQWriter(zmq, port, latch);
		threadPool.submit(zmqw);

		assertTrue("reply was received", latch.await(500, TimeUnit.SECONDS));
		assertTrue("Server was stopped", server.shutdown().awaitSuccess(5, TimeUnit.SECONDS));

		//zmq.destroy();
	}



	@Test
	public void zmqClientServerInteraction() throws InterruptedException {
		final int port = SocketUtils.findAvailableTcpPort();
		final CountDownLatch latch = new CountDownLatch(2);

		TcpServer<Buffer, Buffer>
				zmqs = NetStreams.tcpServer(ZeroMQTcpServer.class, spec -> spec.listen(port));

		zmqs.start(ch ->
						ch.writeWith(ch.log("zmq").take(1).map(buff -> {
							if (buff.remaining() == 12) {
								latch.countDown();
							}
							return Buffer.wrap("Goodbye World!");
						}))
		).await(5, TimeUnit.SECONDS);

		TcpClient<Buffer, Buffer> zmqc = NetStreams.<Buffer, Buffer>tcpClient(ZeroMQTcpClient.class, s -> s
						.connect("127.0.0.1", port)
		);

		final Promise<Buffer> promise = Promises.ready();

		zmqc.start(ch -> {
			ch.log("zmq-c").subscribe(promise);
			return ch.writeWith(Streams.just(Buffer.wrap("Hello World!")));
		}).await(5, TimeUnit.SECONDS);

		String msg = promise
				.await(30, TimeUnit.SECONDS)
				.asString();


		assertThat("messages were exchanged", msg, is("Goodbye World!"));
	}



	private class ZeroMQWriter implements Runnable {

		private final Random random = new Random();
		private final ZContext       zmq;
		private final int            port;
		private final CountDownLatch latch;

		private ZeroMQWriter(ZContext zmq, int port, CountDownLatch latch) {
			this.zmq = zmq;
			this.port = port;
			this.latch = latch;
		}

		@Override
		public void run() {
			String id = UUIDUtils.random()
			                     .toString();
			ZMQ.Socket socket = zmq.createSocket(ZMQ.DEALER);
			socket.setIdentity(id.getBytes());
			socket.connect("tcp://127.0.0.1:" + port);

			byte[] data = new byte[128];
			random.nextBytes(data);

			socket.send(data);

			ZMsg reply = ZMsg.recvMsg(socket);
			log.info("reply: {}", reply);
			latch.countDown();

			//zmq.destroySocket(socket);
		}
	}

}
