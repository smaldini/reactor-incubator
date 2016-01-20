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
package reactor.io.net.impl.zmq;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Processors;
import reactor.core.support.ExecutorUtils;
import reactor.fn.Predicate;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.preprocessor.CodecPreprocessor;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Promise;
import reactor.rx.Streams;
import reactor.rx.net.NetStreams;
import reactor.rx.net.tcp.ReactorTcpClient;
import reactor.rx.net.tcp.ReactorTcpServer;

import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class AbstractNetClientServerTest {

	public static final String LOCALHOST = "127.0.0.1";

	private static final Random random = new Random(System.nanoTime());
	private static final String CHARS  = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789";

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final int senderThreads = Processors.DEFAULT_POOL_SIZE;
	protected Data            data;
	private   ExecutorService serverPool;
	private   ExecutorService clientPool;
	private   int             port;

	protected static Data generateData() {
		char[] name = new char[16];
		for (int i = 0; i < name.length; i++) {
			name[i] = CHARS.charAt(random.nextInt(CHARS.length()));
		}
		return new Data(random.nextInt(), random.nextLong(), new String(name));
	}

	@Before
	public void setup() {
		clientPool = Executors.newCachedThreadPool(ExecutorUtils.newNamedFactory(getClass().getSimpleName() +
		  "-server"));
		serverPool = Executors.newCachedThreadPool(ExecutorUtils.newNamedFactory(getClass().getSimpleName() +
		  "-client"));

		port = SocketUtils.findAvailableTcpPort();

		data = generateData();
	}

	@After
	public void cleanup() throws InterruptedException {
		//env1.shutdown();
		//env2.shutdown();

		clientPool.shutdownNow();
		clientPool.awaitTermination(5, TimeUnit.SECONDS);

		serverPool.shutdownNow();
		serverPool.awaitTermination(5, TimeUnit.SECONDS);
	}

	protected int getPort() {
		return port;
	}

	protected <T> void assertTcpClientServerExchangedData(Class<? extends reactor.io.net.tcp.TcpServer> serverType,
	                                                      Class<? extends reactor.io.net.tcp.TcpClient> clientType,
	                                                      Buffer data) throws InterruptedException {
		assertTcpClientServerExchangedData(
		  serverType,
		  clientType,
		  StandardCodecs.PASS_THROUGH_CODEC,
		  data,
		  (Buffer b) -> {
			  byte[] b1 = data.flip().asBytes();
			  byte[] b2 = b.asBytes();
			  return Arrays.equals(b1, b2);
		  }
		);
	}

	@SuppressWarnings("unchecked")
	protected <T> void assertTcpClientServerExchangedData(Class<? extends reactor.io.net.tcp.TcpServer> serverType,
	                                                      Class<? extends reactor.io.net.tcp.TcpClient> clientType,
	                                                      Codec<Buffer, T, T> codec,
	                                                      T data,
	                                                      Predicate<T> replyPredicate)
	  throws InterruptedException {
		final Codec<Buffer, T, T> elCodec = codec == null ? (Codec<Buffer, T, T>) StandardCodecs.PASS_THROUGH_CODEC :
		  codec;

		ReactorTcpServer<T, T> server = NetStreams.tcpServer(serverType, s -> s.listen(LOCALHOST, getPort())
		                                                                       .preprocessor(CodecPreprocessor.from(elCodec)));

		server.start(ch -> ch.writeWith(ch.take(1))).get();

		ReactorTcpClient<T, T> client = NetStreams.tcpClient(clientType, s -> s
			.connect(LOCALHOST, getPort())
			.preprocessor(CodecPreprocessor.from(elCodec))
		);

		final Promise<T> p = Promise.ready();

		client.start(input -> {
			input.log("echo-in").subscribe(p);
			return input.writeWith(Streams.just(data).log("echo-out")).log("echo.close");
		}).get();


		T reply = p.await(50, TimeUnit.SECONDS);

		assertTrue("reply was correct", replyPredicate.test(reply));

//		assertServerStopped(server);
//		assertClientStopped(client);
	}


	protected static class Data {
		private int    count;
		private long   length;
		private String name;

		public Data() {
		}

		public Data(int count, long length, String name) {
			this.count = count;
			this.length = length;
			this.name = name;
		}

		public int getCount() {
			return count;
		}

		public long getLength() {
			return length;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "Data{" +
			  "count=" + count +
			  ", length=" + length +
			  ", name='" + name + '\'' +
			  '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Data)) return false;

			Data data = (Data) o;

			if (count != data.count) return false;
			if (length != data.length) return false;
			return name.equals(data.name);

		}

		@Override
		public int hashCode() {
			int result = count;
			result = 31 * result + (int) (length ^ (length >>> 32));
			result = 31 * result + name.hashCode();
			return result;
		}
	}

	static {
		System.setProperty("reactor.tcp.selectThreadCount", "2");
		System.setProperty("reactor.tcp.ioThreadCount", "4");
		System.setProperty("reactor.tcp.connectionReactorBacklog", "128");
	}
}
