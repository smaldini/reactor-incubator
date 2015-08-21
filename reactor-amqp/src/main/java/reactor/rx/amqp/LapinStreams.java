package reactor.rx.amqp;

import com.rabbitmq.client.ConnectionFactory;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.rx.Streams;
import reactor.rx.amqp.action.LapinAction;
import reactor.rx.amqp.spec.Exchange;
import reactor.rx.amqp.spec.Queue;
import reactor.rx.amqp.stream.LapinStream;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Create {@link reactor.rx.Stream} from AMQP queues and {@link reactor.rx.action.Action} to AMQP exchanges.
 * This is mainly supporting RabbitMQ (LapinMQ).
 *
 * @author Stephane Maldini
 * @version 0.1
 */
final public class LapinStreams extends Streams {

	/**
	 * Connect to localhost on {@link ConnectionFactory#DEFAULT_AMQP_PORT} and prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 * <p>
	 * Will use default credentials ({@link ConnectionFactory#DEFAULT_USER}/{@link ConnectionFactory#DEFAULT_PASS}
	 *
	 * @param queueName AMQP queue name
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromQueue(String queueName) throws IOException {

		return fromQueue(Queue.lookup(queueName));
	}

	/**
	 * Connect to localhost on {@link ConnectionFactory#DEFAULT_AMQP_PORT} and prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 * <p>
	 * Will use default credentials ({@link ConnectionFactory#DEFAULT_USER}/{@link ConnectionFactory#DEFAULT_PASS}
	 *
	 * @param queueConfig a builder to customize queue declaration, {@link reactor.rx.amqp.spec.Queue#lookup(String)}
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromQueue(Queue queueConfig) throws IOException {
		try {
			return fromQueue(null, queueConfig);
		} catch (NoSuchAlgorithmException | URISyntaxException | KeyManagementException e) {

			//should never happen
			throw new RuntimeException(e);
		}
	}

	/**
	 * Connect to localhost on the given AMQP URI and prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param queueName AMQP queue name
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromQueue(String amqpURI, String queueName)
			throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException {

		return fromQueue(amqpURI, Queue.lookup(queueName));
	}

	/**
	 * Connect to localhost on the given AMQP URI and prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param amqpURI     AMQP URI @see <a href="https://www.rabbitmq.com/uri-spec.html">https://www.rabbitmq.com/uri-spec.html</a>
	 * @param queueConfig a builder to customize queue declaration, {@link reactor.rx.amqp.spec.Queue#lookup(String)}
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromQueue(String amqpURI, Queue queueConfig)
			throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		if (amqpURI != null && !amqpURI.isEmpty()) {
			connectionFactory.setUri(amqpURI);
		}

		return fromLapinAndQueue(Lapin.from(connectionFactory, false), queueConfig, false);
	}

	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin     A ConnectionFactory wrapper that caches a single connection
	 * @param queueName AMQP queue name
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromLapinAndQueue(Lapin lapin, String queueName) {
		return fromLapinAndQueue(lapin, queueName, true);
	}

	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin       A ConnectionFactory wrapper that caches a single connection
	 * @param queueConfig a builder to customize queue declaration, {@link reactor.rx.amqp.spec.Queue#lookup(String)}
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromLapinAndQueue(Lapin lapin, Queue queueConfig) {
		return fromLapinAndQueue(lapin, queueConfig, true);
	}

	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close(). KeepAlive prevents from closing the channel.
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin     A ConnectionFactory wrapper that caches a single connection
	 * @param queueName AMQP queue name
	 * @param keepAlive Keep the connection alive after the last channel is closed
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromLapinAndQueue(Lapin lapin, String queueName, boolean keepAlive) {
		return fromLapinAndQueue(lapin, Queue.lookup(queueName), keepAlive);
	}

	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.Stream} that
	 * will produce data off the given queue. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close(). KeepAlive prevents from closing the channel.
	 * - OnSubscribe signal will be fired after the channel and the queue are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin       A ConnectionFactory wrapper that caches a single connection
	 * @param queueConfig a builder to customize queue declaration, {@link reactor.rx.amqp.spec.Queue#lookup(String)}
	 * @param keepAlive   Keep the connection alive after the last channel is closed
	 * @return {@link reactor.rx.amqp.stream.LapinStream}
	 */
	public static LapinStream fromLapinAndQueue(Lapin lapin, Queue queueConfig, boolean keepAlive) {
		return new LapinStream(lapin, queueConfig, keepAlive);
	}


	/**
	 * Connect to localhost on {@link ConnectionFactory#DEFAULT_AMQP_PORT} and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the default exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 * <p>
	 * Will use default credentials ({@link ConnectionFactory#DEFAULT_USER}/{@link ConnectionFactory#DEFAULT_PASS}
	 *
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toDefaultExchange() throws IOException {

		return toExchange(Exchange.empty());
	}


	/**
	 * Connect to localhost on {@link ConnectionFactory#DEFAULT_AMQP_PORT} and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 * <p>
	 * Will use default credentials ({@link ConnectionFactory#DEFAULT_USER}/{@link ConnectionFactory#DEFAULT_PASS}
	 *
	 * @param exchangeName AMQP exchange name
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toExchange(String exchangeName) throws IOException {

		return toExchange(Exchange.lookup(exchangeName));
	}

	/**
	 * Connect to localhost on {@link ConnectionFactory#DEFAULT_AMQP_PORT} and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 * <p>
	 * Will use default credentials ({@link ConnectionFactory#DEFAULT_USER}/{@link ConnectionFactory#DEFAULT_PASS}
	 *
	 * @param exchangeConfig a builder to customize exchange declaration, {@link reactor.rx.amqp.spec.Exchange#lookup(String)}
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toExchange(Exchange exchangeConfig) throws IOException {
		try {
			return toExchange(null, exchangeConfig);
		} catch (NoSuchAlgorithmException | URISyntaxException | KeyManagementException e) {

			//should never happen
			throw new RuntimeException(e);
		}
	}

	/**
	 * Connect to localhost on the given AMQP URI and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the default exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toDefaultExchange(String amqpURI)
			throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException {

		return toExchange(amqpURI, Exchange.empty());
	}

	/**
	 * Connect to localhost on the given AMQP URI and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param exchangeName AMQP exchange name
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toExchange(String amqpURI, String exchangeName)
			throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException {

		return toExchange(amqpURI, Exchange.lookup(exchangeName));
	}

	/**
	 * Connect to localhost on the given AMQP URI and prepare a {@link reactor.rx.Stream} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param amqpURI     AMQP URI @see <a href="https://www.rabbitmq.com/uri-spec.html">https://www.rabbitmq.com/uri-spec.html</a>
	 * @param exchangeConfig a builder to customize exchange declaration, {@link reactor.rx.amqp.spec.Exchange#lookup(String)}
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toExchange(String amqpURI, Exchange exchangeConfig)
			throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		if (amqpURI != null && !amqpURI.isEmpty()) {
			connectionFactory.setUri(amqpURI);
		}

		return toLapinAndExchange(Lapin.from(connectionFactory, false), exchangeConfig);
	}


	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.action.Action} that
	 * will produce data to the default exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close(). 
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin     A ConnectionFactory wrapper that caches a single connection
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toLapinAndDefaultExchange(Lapin lapin) {
		return toLapinAndExchange(lapin, Exchange.empty());
	}



	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.action.Action} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection
	 * will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin     A ConnectionFactory wrapper that caches a single connection
	 * @param exchangeName AMQP exchange name
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toLapinAndExchange(Lapin lapin, String exchangeName) {
		return toLapinAndExchange(lapin, Exchange.lookup(exchangeName));
	}

	/**
	 * Use the {@link reactor.rx.amqp.Lapin} connection settings to prepare a {@link reactor.rx.action.Action} that
	 * will produce data to the given exchange. Each Stream subscriber will be assigned a different consumer. The
	 * connection will close after the last channel close().
	 * - OnSubscribe signal will be fired after the channel and the exchange are declared if it is the only subscriber
	 * - OnNext signals will be {@link reactor.rx.amqp.signal.QueueSignal} envelopes containing original rabbit data.
	 * - OnError signals will occur on connection errors or cancel notification. It will be the downstream
	 * responsibility to retry.
	 * - OnComplete signals will occur on cancel notification.
	 *
	 * @param lapin       A ConnectionFactory wrapper that caches a single connection
	 * @param exchangeConfig a builder to customize exchange declaration, {@link reactor.rx.amqp.spec.Exchange#lookup(String)}
	 * @return {@link reactor.rx.amqp.action.LapinAction}
	 */
	public static LapinAction toLapinAndExchange(Lapin lapin, Exchange exchangeConfig) {
		return new LapinAction(SynchronousDispatcher.INSTANCE, lapin, exchangeConfig);
	}
}
