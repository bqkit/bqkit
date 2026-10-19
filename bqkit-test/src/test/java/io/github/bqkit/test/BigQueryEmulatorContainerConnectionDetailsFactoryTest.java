package io.github.bqkit.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bqkit.autoconfigure.BqkitAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.service.connection.ConnectionDetailsNotFoundException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.testcontainers.service.connection.ServiceConnectionAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.BigQueryEmulatorContainer;

class BigQueryEmulatorContainerConnectionDetailsFactoryTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ServiceConnectionAutoConfiguration.class, BqkitAutoConfiguration.class));

    @Test
    void standard_emulator_container_is_rejected_because_it_does_not_expose_the_grpc_port() {
        runner.withUserConfiguration(StandardEmulator.class).run(context ->
                assertThat(context).getFailure()
                        .isInstanceOf(ConnectionDetailsNotFoundException.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class StandardEmulator {

        @Bean
        @ServiceConnection
        BigQueryEmulatorContainer emulator() {
            return new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.8.1");
        }
    }
}
