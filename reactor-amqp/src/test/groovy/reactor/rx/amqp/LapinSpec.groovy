package reactor.rx.amqp

import reactor.Environment
import reactor.rx.Streams
import reactor.rx.amqp.signal.ExchangeSignal
import reactor.rx.amqp.spec.Queue
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 */
class LapinSpec extends Specification {

	@Shared
	Environment environment

	void setup() {
		environment = new Environment()
	}

	def cleanup() {
		environment.shutdown()
	}

	def 'A simple stream from rabbit queue'() {
		when:
			'we declare and listen to localhost queue'

			def latch = new CountDownLatch(1)
			def value = ''
			def value2 = ''

			def publisher =	LapinStreams.toDefaultExchange()
				publisher.subscribe()


				LapinStreams.fromQueue(
					Queue.create('test').durable(true).bind('test')
				)
					.qosTolerance(15.0f)
					.log('queue')
					.finallyDo {
						println 'complete'
						value = 'test'
					}
					.consume {
						value = it.toString()

						Streams.just(it.replyTo('hey Bernard'))
						.subscribe(publisher)
					}


		and:
			'a message is published'
			def publisherStream = LapinStreams.toExchange('test')

			Streams.just('hello Bob')
					.map { ExchangeSignal.from(it) }
					.log('publish')
					.subscribe(publisherStream)

			publisherStream
					.replyTo()
					.log('replyTo')
					.consume {
						value2 = it.toString()
						latch.countDown()
					}

		then:
			'it is available'
			latch.await(45, TimeUnit.SECONDS)
			value == 'hello Bob'
			value2 == 'hey Bernard'
	}

}
