package com.developery.azure;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaConsumer {

	@KafkaListener(topics = "hub1", groupId = "myGroup1")
    public void consume(@Payload String message,
    		@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
    		@Header(name = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
    		@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
    		@Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts
    		) {
		log.info("[consumer] Consumed key: {}, partition: {}, offset: {}, topic: {}, ts: {}, message : {}",
				key, partition, offset, topic, ts, message);
		
		if ( true ) throw new RuntimeException("myException");	// exception 처리 확인용으로 추가
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		acknowledgment.acknowledge();  // commit
		log.info("end");
    }
	
	
	@KafkaListener(topics = "hub1.dlt", groupId = "myGroup1")
    public void dltConsume(@Payload String message,
    		@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
    		@Header(name = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
    		@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
    		@Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts
    		) {
		log.info("[dltConsume] dltConsumed key: {}, partition: {}, offset: {}, topic: {}, ts: {}, message : {}",
				key, partition, offset, topic, ts, message);
		
		
		acknowledgment.acknowledge();  // commit
		log.info("end");
    }
}
