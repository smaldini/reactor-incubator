package reactor.rx.amqp.spec;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import reactor.fn.Function;

import java.io.IOException;
import java.util.Map;

/**
 * @author Stephane Maldini
 */
public final class Exchange implements Function<Channel, AMQP.Exchange.DeclareOk> {

	public static final String   TYPE_FANOUT  = "fanout";
	public static final String   TYPE_HEADERS = "headers";
	public static final String   TYPE_DIRECT  = "direct";
	public static final String   TYPE_TOPIC   = "topic";
	public static final Exchange DEFAULT      = Exchange.lookup("");

	private final String              exchange;
	private final boolean             passive;
	private       String              type;
	private       boolean             durable;
	private       boolean             autoDelete;
	private       Map<String, Object> arguments;
	//boolean internal,

	public static Exchange empty() {
		return DEFAULT;
	}

	public static Exchange lookup(String exchange) {
		return new Exchange(exchange, true);
	}

	public static Exchange create(String exchange) {
		return new Exchange(exchange, false);
	}

	Exchange(String exchange, boolean passive) {
		this.exchange = exchange;
		this.passive = passive;

		if (!passive) {
			fanOut();
		}
	}

	public String exchange() {
		return exchange;
	}

	public String type() {
		return type;
	}

	public Exchange type(String type) {
		this.type = type;
		return this;
	}


	public boolean durable() {
		return durable;
	}

	public Exchange durable(boolean durable) {
		this.durable = durable;
		return this;
	}


	public boolean autoDelete() {
		return autoDelete;
	}

	public Exchange autoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
		return this;
	}

	public Exchange topic() {
		return type(TYPE_TOPIC);
	}

	public Exchange fanOut() {
		return type(TYPE_FANOUT);
	}

	public Exchange direct() {
		return type(TYPE_DIRECT);
	}

	public Exchange headers() {
		return type(TYPE_HEADERS);
	}


	public Map<String, Object> arguments() {
		return arguments;
	}

	public Exchange arguments(Map<String, Object> arguments) {
		this.arguments = arguments;
		return this;
	}

	@Override
	public AMQP.Exchange.DeclareOk apply(Channel channel) {
		try {
			if(this == DEFAULT){
				return null;
			} else if (passive) {
				return channel.exchangeDeclarePassive(exchange);
			} else {
				return channel.exchangeDeclare(exchange, type, autoDelete, durable, arguments);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
