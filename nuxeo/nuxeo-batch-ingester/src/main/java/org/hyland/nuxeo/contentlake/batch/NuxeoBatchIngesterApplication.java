package org.hyland.nuxeo.contentlake.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "org.hyland.contentlake")
public class NuxeoBatchIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(NuxeoBatchIngesterApplication.class, args);
    }
}
