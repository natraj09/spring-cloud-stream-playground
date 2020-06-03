package reactive.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
@RestController
public class ReactiveProcessorApplication {

	@Autowired
	private ObjectMapper jsonMapper;

	 Logger log = LoggerFactory.getLogger(ReactiveProcessorApplication.class);

	private final EmitterProcessor<Message<?>> processor = EmitterProcessor.create();

	public static void main(String[] args) {
		SpringApplication.run(ReactiveProcessorApplication.class, args);
	}

	private static final String X_RETRIES_HEADER = "x-retries";

	@Autowired
	RestTemplateBuilder restTemplate;


	@Bean
	public Function<Flux<Message<Map>>, Flux<Message<Map>>> testProcess() {
		return data -> data.flatMap(inbound -> {
			try {
				restTemplate.build().getForObject(inbound.getPayload().get("url").toString(), Object.class);
				return Mono.just(MessageBuilder.fromMessage(inbound).setHeader("spring.cloud.stream.sendto.destination", "rest-output").build());
			} catch (Exception e) {
				return Mono.just(MessageBuilder.fromMessage(inbound)
						.setHeader(X_RETRIES_HEADER, new Integer(0))
						.setHeader("spring.cloud.stream.sendto.destination", "rest-input-error").build());
			}
		});
	}


//	@Bean
//	public Function<Message<Map>, Message<Map>> testProcess() {
//		return inbound -> {
//			try {
//				restTemplate.build().getForObject(inbound.getPayload().get("url").toString(), Object.class);
//				return MessageBuilder.fromMessage(inbound).setHeader("spring.cloud.stream.sendto.destination", "rest-output").build();
//			} catch (Exception e) {
//				return MessageBuilder.fromMessage(inbound)
//						.setHeader(X_RETRIES_HEADER, new Integer(0))
//						.setHeader("spring.cloud.stream.sendto.destination", "rest-input-error").build();
//			}
//		};
//	}



	@Bean
	public Function<Flux<Message<Map>>, Flux<Message<Map>>> testError() {
		return data -> data.flatMap(inbound -> {
			try {
				restTemplate.build().getForObject(inbound.getPayload().get("url").toString(), String.class);
				return Mono.just(MessageBuilder.fromMessage(inbound).build());
			} catch (Exception e) {
				Integer retries = inbound.getHeaders().get(X_RETRIES_HEADER, Integer.class);
				if (retries < 2) {
					return Mono.just(MessageBuilder.fromMessage(inbound)
							.setHeader(X_RETRIES_HEADER, new Integer(retries + 1))
							.setHeader("spring.cloud.stream.sendto.destination", "rest-input-error").build());
				}
				// Handle retries < N
				else {
					return Mono.just(MessageBuilder.fromMessage(inbound).build());
				}
			}
		});
	}


	@SuppressWarnings("unchecked")
	@RequestMapping(path = "/", method = RequestMethod.POST, consumes = "*/*")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void handleRequest(@RequestBody String body, @RequestHeader(HttpHeaders.CONTENT_TYPE) Object contentType) throws Exception {
		Map<String, String> payload = jsonMapper.readValue(body, Map.class);
		Message<?> message = MessageBuilder.withPayload(payload).build();
		processor.onNext(message);
	}

	@Bean
	public Supplier<Flux<Message<?>>> testSource() {
		return () -> processor;
	}

	//Following sink is used as test consumer. It logs the data received through the consumer.
	static class TestSink {
		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Consumer<Message<Map>> testSink() {
			return data -> logger.info("Data received" + data);
		}
	}

}
