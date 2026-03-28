package org.hyland.nuxeo.contentlake.live;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication(scanBasePackages = {
        "org.hyland.contentlake",
        "org.hyland.nuxeo.contentlake"
})
public class NuxeoLiveIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(NuxeoLiveIngesterApplication.class, args);
    }
}
