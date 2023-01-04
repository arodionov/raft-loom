package io.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class RaftApplication {

	public static void main(String[] args) {
		SpringApplication.run(RaftApplication.class, args);
	}

	@RestController
	static class MyController {

		@GetMapping("/")
		String hello() throws InterruptedException {

			// Simulate a blocking call for one second. The thread should be put aside for about a second.
			Thread.sleep(1_000);
			Thread thread = Thread.currentThread();
			return "OK: " + thread;
		}

	}

}
