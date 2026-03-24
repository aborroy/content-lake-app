package org.alfresco.contentlake.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "org.alfresco.contentlake")
public class BatchIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchIngesterApplication.class, args);
    }
}
