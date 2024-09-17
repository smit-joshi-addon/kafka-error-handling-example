package com.kafka.error.handling.pubisher;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.kafka.error.handling.dto.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaMessagePublisher {

	@Value("${app.topic.name}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	
	public void sendEvents(User user) {
		try {
			CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, user);
			future.whenComplete((result, ex) -> {
				if (ex == null) {
					System.out.println(
							"Sent message=[" + user + "] with offset=[" + result.getRecordMetadata().offset() + "]");
				} else {
					System.out.println("Unable to send message=[" + user + "] due to:" + ex.getLocalizedMessage());
				}
			});
		} catch (Exception e) {
			log.error("Error {}", e.getLocalizedMessage());
		}
	}

}
