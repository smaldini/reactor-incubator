package reactor.rx.amqp.signal;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import reactor.fn.Supplier;

/**
 * A little container for rabbit incoming messages
 *
 * @author Stephane Maldini
 */
public final class QueueSignal implements Supplier<byte[]>{

	public static final String DEFAULT_REPLYTO_EXCHANGE = "";

	final byte[] body;
	final String consumerTag;
	final Envelope envelope;
	final AMQP.BasicProperties properties;

	public static QueueSignal from(byte[] body, String consumerTag, Envelope envelope, AMQP.BasicProperties properties) {
		return new QueueSignal(body, consumerTag, envelope, properties);
	}

	QueueSignal(byte[] body, String consumerTag, Envelope envelope, AMQP.BasicProperties properties) {
		this.body = body;
		this.consumerTag = consumerTag;
		this.envelope = envelope;
		this.properties = properties;
	}

	public ExchangeSignal replyTo(String body) {
		return replyTo(body.getBytes(), DEFAULT_REPLYTO_EXCHANGE, MessageProperties.TEXT_PLAIN);
	}

	public ExchangeSignal replyTo(byte[] body, AMQP.BasicProperties properties) {
		return replyTo(body, DEFAULT_REPLYTO_EXCHANGE, properties);
	}

	public ExchangeSignal replyTo(byte[] body, String exchange, AMQP.BasicProperties properties) {
		return new ExchangeSignal(body, exchange, this.properties.getReplyTo(), false, properties);
	}

	@Override
	public byte[] get() {
		return body;
	}

	public String consumerTag() {
		return consumerTag;
	}

	public Envelope envelope() {
		return envelope;
	}

	public AMQP.BasicProperties properties() {
		return properties;
	}

	@Override
	public String toString(){
		return new String(body);
	}
}
