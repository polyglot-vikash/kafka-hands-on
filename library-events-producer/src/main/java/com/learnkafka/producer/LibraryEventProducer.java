package com.learnkafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;


@Component
@Slf4j
public class LibraryEventProducer {
    // here we have chosen integer, String because of key and value serilizer
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws Exception {
        Integer key = libraryEvent.getLibraryEventId();
        // converts the object to JSON representation string
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            private void handleFailure(Integer key, String value, Throwable ex) {
                log.error("Error sending the message and the exception is {}" + ex.getMessage());
                try {
                    throw ex;
                } catch (Throwable throwable) {
                    log.error("Error on failure{}" + throwable.getMessage());
                }
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

        log.info("Message sent successfully for the key:{} and value is :{}, partition is :{}", key, value, result.getRecordMetadata().partition());
    }

    public SendResult<Integer, String> sendLibraryEventSynchronus(LibraryEvent libraryEvent) throws Exception {
        Integer key = libraryEvent.getLibraryEventId();
        SendResult<Integer, String> sendResult = null;
        // converts the object to JSON representation string
        String value = objectMapper.writeValueAsString(libraryEvent);
        try {
            // .get will wait until future is reslved to success or failure , timeout support is added
            sendResult = kafkaTemplate.sendDefault(key, value).get(2, TimeUnit.SECONDS);
            log.info("Message sent successfully for the key:{} and value is :{}, partition is :{}", key, value, sendResult.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Exception while sending the message " + e.getMessage());
            throw new RuntimeException(e);
        }

        return sendResult;

    }


    // This method sends the message to the given topic
    public void sendLibraryEventToTopic(LibraryEvent libraryEvent, String topic) throws Exception {
        Integer key = libraryEvent.getLibraryEventId();
        // converts the object to JSON representation string
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(topic, key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            private void handleFailure(Integer key, String value, Throwable ex) {
                log.error("Error sending the message and the exception is {}" + ex.getMessage());
                try {
                    throw ex;
                } catch (Throwable throwable) {
                    log.error("Error on failure{}" + throwable.getMessage());
                }
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    public void sendLibraryEventToTopicUsingProducerRecord(LibraryEvent libraryEvent, String topic) throws Exception {
        Integer key = libraryEvent.getLibraryEventId();
        // converts the object to JSON representation string
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }


    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is {}" + ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error on failure{}" + throwable.getMessage());
        }
    }

}
