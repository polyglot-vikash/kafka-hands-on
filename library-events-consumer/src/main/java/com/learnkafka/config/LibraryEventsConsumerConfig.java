package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.hibernate.HibernateException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory) kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(retryContext -> {
            // only recover for HIbernateException
            if(retryContext.getLastThrowable().getCause() instanceof HibernateException) {
                // recovery logic , applied only for Hibernate related exception
                log.info("Inside recoverable logic");
            }
            else {
                log.info("Inside the non recoverable logic");
                throw new RuntimeException(retryContext.getLastThrowable().getMessage());
            }
        });
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate  = new RetryTemplate();
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        // This map contains the Exception for which retry method should be applied , for true it will apply the retries and for false it will not
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(IllegalArgumentException.class, false);
        exceptionMap.put(HibernateException.class, true);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionMap, true);
        return simpleRetryPolicy;
    }

}

