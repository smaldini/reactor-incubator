package reactor.rx.amqp.stream;

import com.rabbitmq.client.Channel;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Stream;
import reactor.rx.amqp.Lapin;
import reactor.rx.amqp.signal.QueueSignal;
import reactor.rx.amqp.spec.Queue;
import reactor.rx.amqp.subscription.LapinQueueSubscription;
import reactor.rx.amqp.subscription.LapinQueueWithQosSubscription;
import reactor.rx.subscription.PushSubscription;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Stream that emits {@link Iterable} values one by one and then complete.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * LapinStreams.fromQueue(lapin, "queueId").subscribe(
 *log::info,
 *log::error,
 * (-> log.info("complete"))
 * )
 * }
 * <p>
 * Will log:
 * 1
 * 2
 * 3
 * 4
 * complete
 *
 * @author Stephane Maldini
 */
public class LapinStream extends Stream<QueueSignal> {

	public static final Long  DEFAULT_MAX_QOS   = 8192l;
	public static final Long  DEFAULT_MIN_QOS   = 1l;
	public static final Float DEFAULT_TOLERANCE = 50.0f;

	private final Lapin   lapin;
	private final Queue   queueConfig;
	private final boolean keepAlive;
	private final AtomicInteger refCount = new AtomicInteger(0);

	private Channel             channel;
	private Map<String, Object> consumerArguments;
	private boolean             bindAckToRequest;
	private Long                maxQos;
	private Long                minQos;
	private Float               qosTolerance;

	public LapinStream(Lapin lapin, Queue queueConfig, boolean keepAlive) {
		this.lapin = lapin;
		this.queueConfig = queueConfig;
		this.keepAlive = keepAlive;
	}

	public LapinStream consumerArguments(Map<String, Object> consumerArguments) {
		this.consumerArguments = consumerArguments;
		return this;
	}

	public LapinStream maxQos(Long maxQos) {
		this.maxQos = maxQos;
		return this;
	}

	public Long maxQos() {
		return maxQos == null ? DEFAULT_MAX_QOS : maxQos;
	}

	public LapinStream minQos(Long minQos) {
		this.minQos = minQos;
		return this;
	}

	public Long minQos() {
		return minQos == null ? DEFAULT_MIN_QOS : minQos;
	}

	public LapinStream qosTolerance(Float qosTolerance) {
		this.qosTolerance = qosTolerance;
		return this;
	}

	public Float qosTolerance() {
		return qosTolerance == null ? DEFAULT_TOLERANCE : qosTolerance;
	}

	public Queue queueConfig() {
		return queueConfig;
	}

	public boolean bindAckToRequest() {
		return bindAckToRequest;
	}

	public LapinStream bindAckToRequest(boolean bindAckToRequest) {
		this.bindAckToRequest = bindAckToRequest;
		return this;
	}

	public boolean qosMode() {
		return qosTolerance != null || minQos != null || maxQos != null;
	}

	@Override
	public boolean cancelSubscription(PushSubscription<QueueSignal> petitMessagePushSubscription) {
		if (this.channel != null) {
			try {
				if (refCount.decrementAndGet() == 0) {
					lapin.destroyChannel(channel);
				}
			} catch (IOException e) {

				return false;
			}
		}
		return true;
	}

	@Override
	public void subscribe(final Subscriber<? super QueueSignal> subscriber) {
		try {
			if (refCount.getAndIncrement() == 0) {
				this.channel = lapin.createChannel();
			}
			subscriber.onSubscribe(createSubscription(subscriber, null, null));
		} catch (Exception e) {
			refCount.incrementAndGet();
			subscriber.onError(e);
		}
	}


	protected PushSubscription<QueueSignal> createSubscription(Subscriber<? super QueueSignal> subscriber,
	                                                           Subscription dependency,
	                                                           java.util.function.Consumer<QueueSignal> doOnNext) {
		LapinQueueSubscription lapinQueueSubscription;

		if (!qosMode()) {
			lapinQueueSubscription = new LapinQueueSubscription(
					this, subscriber, queueConfig, bindAckToRequest, consumerArguments, dependency, doOnNext);
		} else {
			lapinQueueSubscription = new LapinQueueWithQosSubscription(
					this, subscriber, queueConfig, minQos(), maxQos(), qosTolerance(), bindAckToRequest, consumerArguments,
					dependency, doOnNext);
		}
		lapinQueueSubscription.channel(channel);

		return lapinQueueSubscription;
	}

	@Override
	public String toString() {
		return "{lapin=" + lapin + ", queue=" + queueConfig.queue() + " subscribers=" + refCount + ", " +
				"channel=" + channel + "}";
	}

}
