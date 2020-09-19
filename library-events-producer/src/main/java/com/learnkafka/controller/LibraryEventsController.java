package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEnumType;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LibraryEventsController {
    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/asynclibraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        try {
            log.info("before send library event");
            libraryEvent.setLibraryEnumType(LibraryEnumType.NEW);
            libraryEventProducer.sendLibraryEvent(libraryEvent);
            log.info("after send library event");
            return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
        } catch (Exception ex) {
            log.error("Error while sending the message: " + ex.getMessage());
            throw new RuntimeException(ex);
        }


    }

    @PostMapping("/v1/synclibraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronus(@RequestBody LibraryEvent libraryEvent) {
        try {
            log.info("before send library event");
            libraryEvent.setLibraryEnumType(LibraryEnumType.NEW);
            libraryEventProducer.sendLibraryEventSynchronus(libraryEvent);
            log.info("after send library event");
            return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
        } catch (Exception ex) {
            log.error("Error while sending the message: " + ex.getMessage());
            throw new RuntimeException(ex);
        }


    }

    @PostMapping("/v1/asynclibraryeventtotopic")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronusToTopic(@RequestBody LibraryEvent libraryEvent) {
        try {
            log.info("before send library event");
            String topic = "library-events";
            libraryEvent.setLibraryEnumType(LibraryEnumType.NEW);
            libraryEventProducer.sendLibraryEventToTopic(libraryEvent, topic);
            log.info("after send library event");
            return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
        } catch (Exception ex) {
            log.error("Error while sending the message: " + ex.getMessage());
            throw new RuntimeException(ex);
        }


    }

    @PostMapping("/v1/asyncprocuderrecord")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronusToTopicUsingProducerRecord(@RequestBody LibraryEvent libraryEvent) {
        try {
            log.info("before send library event");
            libraryEvent.setLibraryEnumType(LibraryEnumType.NEW);
            libraryEventProducer.sendLibraryEventToTopicUsingProducerRecord(libraryEvent, "library-events");
            log.info("after send library event");
            return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
        } catch (Exception ex) {
            log.error("Error while sending the message: " + ex.getMessage());
            throw new RuntimeException(ex);
        }


    }

    @PutMapping("/v1/updatedRecord")
    public ResponseEntity<?> updateRecord(@RequestBody LibraryEvent libraryEvent) throws Exception
    {
        if(libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event id");
        }

        libraryEvent.setLibraryEnumType(LibraryEnumType.UPDATE);
        libraryEventProducer.sendLibraryEventToTopicUsingProducerRecord(libraryEvent, "library-events");
        log.info("Library event updated");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    @GetMapping("/v1/test")
    public String test() {
        return "Hello";
    }
}
