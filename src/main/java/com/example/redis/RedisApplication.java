package com.example.redis;

import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(RedisApplication.class, args);
		System.in.read();
	}
}
