package com.kafka.error.handling.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
	private Integer id;
	private String firstName;
	private String lastName;
	private String email;
	private String gender;
	private String ipAddress;
}
