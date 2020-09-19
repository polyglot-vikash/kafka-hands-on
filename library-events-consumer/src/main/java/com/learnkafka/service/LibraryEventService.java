package com.learnkafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.HibernateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository repository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws Exception{
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library event : {} ", libraryEvent);
        switch (libraryEvent.getLibraryEnumType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Id can not be null for update");
        }
        Optional<LibraryEvent> libraryEventObj =  repository.findById(libraryEvent.getLibraryEventId());

        if(!libraryEventObj.isPresent()) {
            throw new IllegalArgumentException("Library event is null");
        }

        log.info("Validation is successful for library event " + libraryEvent.getLibraryEventId());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        try{
            repository.save(libraryEvent);
            log.info("Successfully persisted library event: {} ", libraryEvent);
        }catch (Exception e) {
            log.error("Unable to save/update the object");
            throw new HibernateException("Unable to save the library event");
        }

    }
}
