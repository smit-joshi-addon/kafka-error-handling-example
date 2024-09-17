package com.kafka.error.handling.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.error.handling.dto.User;
import com.kafka.error.handling.pubisher.KafkaMessagePublisher;

@RequestMapping("/api/event/")
@RestController
public class EventsController {

	@Autowired
	private KafkaMessagePublisher publish;

	@PostMapping("/publish")
	public void publishEvent(@RequestBody User user) {
		publish.sendEvents(user);
	}

}
