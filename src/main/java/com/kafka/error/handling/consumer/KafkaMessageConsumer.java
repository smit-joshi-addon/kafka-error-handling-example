package com.kafka.error.handling.consumer;

import java.util.List;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.kafka.error.handling.dto.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaMessageConsumer {

	// N - 1 times retry
	@RetryableTopic(attempts = "4")
//	@RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000))
//	@RetryableTopic(attempts = "4", exclude = { NullPointerException.class, RuntimeException.class })
	@KafkaListener(topics = "${app.topic.name}", groupId = "sj-consumer-group")
	public void consumeEvents(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {

		log.info("Received: {}, from: {} , offset: {}", user, topic, offset);
		// Validate restricted ip before process the records
		List<String> restrictedIpList = List.of("32.241.244.236", "32.241.244.226", "32.221.244.236");
		if (restrictedIpList.contains(user.getIpAddress())) {
			throw new RuntimeException("Invalid IP Address");
		}

	}

	@DltHandler
	public void listenDLT(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {
		log.info("DLT received: {} , from {} , offset {}", user.getFirstName(), topic, offset);
	}

}
