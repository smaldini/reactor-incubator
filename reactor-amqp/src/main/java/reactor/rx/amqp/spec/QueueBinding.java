package reactor.rx.amqp.spec;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import reactor.fn.Function;

import java.io.IOException;
import java.util.Map;

/**
 * @author Stephane Maldini
 */
public final class QueueBinding implements Function<Channel, AMQP.Queue.BindOk> {

	public static final String ROUTING_KEY_ALL  = "#";
	public static final String ROUTING_KEY_ROOT = "*";

	private final String              exchange;
	private       String              queue;
	private       String              routingKey;
	private       Map<String, Object> arguments;

	public static QueueBinding with(String name, String exchange) {
		return new QueueBinding(name, exchange);
	}

	QueueBinding(String queue, String exchange) {
		this.queue = queue;
		this.exchange = exchange;
		all();
	}

	public String queue() {
		return queue;
	}

	public QueueBinding queue(String queue) {
		this.queue = queue;
		return this;
	}

	public String exchange() {
		return exchange;
	}

	public String routingKey() {
		return routingKey;
	}

	public QueueBinding routingKey(String routingKey) {
		this.routingKey = routingKey;
		return this;
	}

	public QueueBinding all() {
		return routingKey(ROUTING_KEY_ALL);
	}

	public QueueBinding rootKeys() {
		return routingKey(ROUTING_KEY_ROOT);
	}


	public Map<String, Object> arguments() {
		return arguments;
	}

	public QueueBinding arguments(Map<String, Object> arguments) {
		this.arguments = arguments;
		return this;
	}

	@Override
	public AMQP.Queue.BindOk apply(Channel channel) {
		try {
			return channel.queueBind(queue, exchange, routingKey, arguments);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
