package reactor.rx.amqp.signal;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;
import reactor.fn.Supplier;

/**
 * A little container for rabbit outgoing messages
 *
 * @author Stephane Maldini
 */
public final class ExchangeSignal implements Supplier<byte[]> {

	final byte[]               body;
	final String               exchange;
	final String               routingKey;
	final boolean              mandatory;
	final AMQP.BasicProperties properties;

	public static ExchangeSignal from(String body) {
		return from(body.getBytes(), MessageProperties.TEXT_PLAIN);
	}

	public static ExchangeSignal from(byte[] body) {
		return route(body, null);
	}

	public static ExchangeSignal from(byte[] body, AMQP.BasicProperties properties) {
		return route(body, null, null, properties);
	}

	public static ExchangeSignal from(String body, AMQP.BasicProperties properties) {
		return route(body.getBytes(), null, null, properties);
	}

	public static ExchangeSignal route(byte[] body, String routingKey) {
		return route(body, routingKey, null);
	}

	public static ExchangeSignal route(String body, String routingKey) {
		return route(body.getBytes(), routingKey, null, MessageProperties.TEXT_PLAIN);
	}

	public static ExchangeSignal route(byte[] body, String routingKey, String exchange) {
		return route(body, routingKey, exchange, null);
	}

	public static ExchangeSignal route(String body, String routingKey, String exchange) {
		return route(body.getBytes(), routingKey, exchange, MessageProperties.TEXT_PLAIN);
	}

	public static ExchangeSignal route(byte[] body, String routingKey, String exchange,
	                                   AMQP.BasicProperties properties) {
		return new ExchangeSignal(body, exchange, routingKey, false, properties);
	}

	public static ExchangeSignal mandatory(byte[] body, String routingKey, String exchange) {
		return mandatory(body, exchange, routingKey, null);
	}

	public static ExchangeSignal mandatory(String body, String routingKey, String exchange) {
		return mandatory(body.getBytes(), exchange, routingKey, MessageProperties.TEXT_PLAIN);
	}

	public static ExchangeSignal mandatory(byte[] body, String routingKey, String exchange,
	                                       AMQP.BasicProperties properties) {
		return new ExchangeSignal(body, exchange, routingKey, true, properties);
	}

	ExchangeSignal(byte[] body, String exchange, String routingKey, boolean mandatory,
	               AMQP.BasicProperties properties) {
		this.body = body;
		this.exchange = exchange;
		this.routingKey = routingKey;
		this.mandatory = mandatory;
		this.properties = properties;
	}

	@Override
	public byte[] get() {
		return body;
	}

	public String routingKey() {
		return routingKey;
	}

	public boolean mandatory() {
		return mandatory;
	}

	public AMQP.BasicProperties properties() {
		return properties;
	}

	public String exchange() {
		return exchange;
	}

	@Override
	public String toString() {
		return "ExchangeSignal{" +
				"body=" + new String(body) +
				", exchange='" + exchange + '\'' +
				", routingKey='" + routingKey + '\'' +
				", mandatory=" + mandatory +
				", properties=" + properties +
				'}';
	}
}
