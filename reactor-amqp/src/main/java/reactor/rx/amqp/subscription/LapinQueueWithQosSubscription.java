package reactor.rx.amqp.subscription;

import java.io.IOException;
import java.util.Map;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.util.Assert;
import reactor.rx.amqp.signal.QueueSignal;
import reactor.rx.amqp.spec.Queue;
import reactor.rx.amqp.stream.LapinStream;

/**
 * @author Stephane Maldini
 */
public class LapinQueueWithQosSubscription extends LapinQueueSubscription {

	private final Long  maxQos;
	private final Long  minQos;
	private final Float qosTolerance;

	public LapinQueueWithQosSubscription(LapinStream lapinStream, Subscriber<? super QueueSignal> subscriber,
	                                     Queue queue,
	                                     Long minQos,
	                                     Long maxQos,
	                                     Float qosTolerance,
	                                     boolean bindAckToRequest,
	                                     Map<String, Object> consumerArguments,
	                                     Subscription dependency,
	                                     java.util.function.Consumer<QueueSignal> doOnNext
	) {
		super(lapinStream, subscriber, queue, bindAckToRequest, consumerArguments, dependency, doOnNext);
		Assert.isTrue(minQos > 0);
		Assert.isTrue(maxQos > minQos);
		Assert.isTrue(qosTolerance > 0);
		this.maxQos = maxQos;
		this.minQos = minQos;
		this.qosTolerance = qosTolerance;
	}

	@Override
	protected void onRequest(long elements) {
		preRequest(elements);
		long lastRequest = this.lastRequest;

		if(channel != null) {
			float delta = Math.abs(elements - lastRequest) / elements;
			if(delta > qosTolerance){
				try {
					channel.basicQos((int) Math.min(maxQos, Math.max(minQos, elements)));
				} catch (IOException e) {
					onError(e);
				}
			}

		}
	}

}
