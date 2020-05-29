package reactive.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

	@Bean
	public Function<Flux<Map>, Flux<Map>> testProcess() {
		return inbound -> inbound.
				log()
				.flatMap(data -> {
					if(data.get("error")!=null){
						log.info("inside error");
						return Mono.error(new RuntimeException(data.toString()));
					} else {
						return Mono.just(data);
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
		private final Log logger = LogFactory.getLog(getClass());

		@Bean
		public Consumer<Map> testSink() {
			return data -> logger.info("Data received" + data);
		}
	}
}
