package reactor.rx.amqp.spec;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Stephane Maldini
 */
public final class Queue {

	private final boolean             passive;
	private final boolean             anonymous;
	private       String              queue;
	private       boolean             durable;
	private       boolean             exclusiveQueue;
	private       boolean             autoDelete;
	private       Map<String, Object> queueParameters;
	private       List<QueueBinding>  bindings;

	public static Queue temp() {
		return create(null).autoDelete(true).durable(false);
	}

	public static Queue lookup(String name) {
		return new Queue(name, true);
	}

	public static Queue create(String name) {
		return new Queue(name, false);
	}

	Queue(String queue, boolean passive) {
		this.queue = queue;
		this.passive = passive;
		this.anonymous = queue == null;
	}

	public String queue() {
		return queue;
	}

	public boolean durable() {
		return durable;
	}

	public Queue durable(boolean durable) {
		this.durable = durable;
		return this;
	}

	public boolean exclusiveQueue() {
		return exclusiveQueue;
	}

	public Queue exclusiveQueue(boolean exclusiveQueue) {
		this.exclusiveQueue = exclusiveQueue;
		return this;
	}

	public boolean autoDelete() {
		return autoDelete;
	}

	public boolean passive() {
		return passive;
	}

	public Queue autoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
		return this;
	}

	public Map<String, Object> queueParameters() {
		return queueParameters;
	}

	public Queue queueParameters(Map<String, Object> queueParameters) {
		this.queueParameters = queueParameters;
		return this;
	}

	public List<QueueBinding> bindings() {
		return bindings;
	}

	public boolean anonymous() {
		return anonymous;
	}

	public Queue bind(String exchange) {
		return bind(QueueBinding.with(queue, exchange));
	}

	public Queue bind(QueueBinding queueBinding) {
		if (bindings == null) {
			bindings = new ArrayList<>();
		}
		bindings.add(queueBinding);
		return this;
	}

	public AMQP.Queue.DeclareOk bind(Channel channel) throws Exception {
		AMQP.Queue.DeclareOk declareOk;

		if (anonymous && queue == null) {
			declareOk = channel.queueDeclare();
			this.queue = declareOk.getQueue();
		} else if (!anonymous && !passive) {
			declareOk = channel.queueDeclare(queue, durable, exclusiveQueue, autoDelete, queueParameters);
		} else {
			declareOk = channel.queueDeclarePassive(queue);
		}

		if (bindings != null && !bindings.isEmpty()) {
			for (QueueBinding bindingConfig : bindings) {
				bindingConfig.queue(queue).apply(channel);
			}
		}

		return declareOk;
	}
}
