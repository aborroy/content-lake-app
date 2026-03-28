package org.hyland.alfresco.contentlake.live;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
        "org.hyland.contentlake",
        "org.hyland.alfresco.contentlake"
})
public class LiveIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiveIngesterApplication.class, args);
    }
}
