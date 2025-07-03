package org.example.filesystem;

import io.etcd.jetcd.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class FileSystemApplication {

    public static void main(String[] args) {SpringApplication.run(FileSystemApplication.class, args);}

    @Bean
    public Client getClient(@Value("${etcd.url}") String etcdUrl) {
        return Client
                .builder()
                .endpoints(etcdUrl)
                .build();
    }
}
