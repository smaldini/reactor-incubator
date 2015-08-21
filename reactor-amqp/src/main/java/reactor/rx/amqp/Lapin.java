package reactor.rx.amqp;

import com.rabbitmq.client.*;
import reactor.fn.tuple.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RabbitMQ connection holder and channel provider.
 *
 * @author Stephane Maldini
 */
public class Lapin {

	private final ConnectionFactory connectionFactory;
	private final AtomicInteger refCount = new AtomicInteger(0);
	private final boolean keepAlive;
	private volatile Connection connection;

	public static Lapin from(ConnectionFactory connectionFactory) throws IOException {
		return from(connectionFactory, null, null, false);
	}

	public static Lapin from(ConnectionFactory connectionFactory, boolean keepAlive) throws IOException {
		return from(connectionFactory, null, null, keepAlive);
	}

	public static Lapin from(ConnectionFactory connectionFactory, List<Tuple2<String,
			Integer>> addresses) throws IOException {
		return from(connectionFactory, addresses, null, false);
	}

	public static Lapin from(ConnectionFactory connectionFactory, List<Tuple2<String,
			Integer>> addresses, boolean keepAlive) throws IOException {
		return from(connectionFactory, addresses, null, keepAlive);
	}

	public static Lapin from(ConnectionFactory connectionFactory, List<Tuple2<String, Integer>> addresses,
	                         ExecutorService executorService) throws IOException {
		return from(connectionFactory, addresses, executorService, false);
	}

	public static Lapin from(ConnectionFactory connectionFactory, List<Tuple2<String, Integer>> addresses,
	                         ExecutorService executorService, boolean keepAlive) throws IOException {
		return new Lapin(connectionFactory, addresses, executorService, keepAlive);
	}

	Lapin(ConnectionFactory connectionFactory,
	      List<Tuple2<String, Integer>> addresses,
	      ExecutorService executorService,
	      boolean keepAlive) throws IOException {

		this.connectionFactory = connectionFactory == null ? new ConnectionFactory() : connectionFactory;
		this.keepAlive = keepAlive;

		if (addresses != null) {
			if (executorService == null) {
				refreshConnection(addresses);
			} else {
				refreshConnection(addresses, executorService);
			}
		} else {
			refreshConnection();
		}
	}

	public final void close() throws IOException {
		if (this.connection != null) {
			this.connection.close();
		}
	}

	public final void refreshConnection() throws IOException {
		close();
		this.connection = connectionFactory.newConnection();
	}


	public final void refreshConnection(List<Tuple2<String, Integer>> addresses) throws IOException {
		close();
		this.connection = connectionFactory.newConnection(fromTupleList(addresses));
	}

	public final void refreshConnection(List<Tuple2<String, Integer>> addresses, ExecutorService executorService)
			throws IOException {
		close();
		this.connection = connectionFactory.newConnection(executorService, fromTupleList(addresses));
	}

	public final ConnectionFactory connectionFactory() {
		return connectionFactory;
	}

	public final Connection connection() {
		return connection;
	}

	public Channel createChannel() throws IOException {
		if (refCount.getAndIncrement() == 0) {
			refreshConnection();
		}
		return connection.createChannel();
	}

	public void destroyChannel(Channel channel) throws IOException {
		try{
			channel.close();
		} catch (AlreadyClosedException alreadyClosedException){
			//IGNORE
		} finally {
			if (refCount.decrementAndGet() == 0){
				close();
			}
		}

	}

	public static Address[] fromTupleList(List<Tuple2<String, Integer>> addresses) {
		Address[] arrayAddresses = new Address[addresses.size()];
		Tuple2<String, Integer> cursor;

		for (int i = 0; i < arrayAddresses.length; i++) {
			cursor = addresses.get(i);
			arrayAddresses[i] = new Address(cursor.getT1(), cursor.getT2());
		}
		return arrayAddresses;
	}

	@Override
	public String toString() {
		return "Lapin{" +
				"refCount=" + refCount +
				", keepAlive=" + keepAlive +
				'}';
	}
}
