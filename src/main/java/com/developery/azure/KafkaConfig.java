package com.developery.azure;


import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@AllArgsConstructor
public class KafkaConfig {
	
	final ConsumerFactory<String, Object> consumerFactory;		// 자동 주입됨.
	
	final KafkaTemplate<String, Object> kafkaTemplate;
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(2);		// 이곳의 숫자만큼 consumer thread가 구동됨. 
		
		// deprecated 인데 예제들이 대부분 이거라서... 
		@SuppressWarnings("deprecation")
		final DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
		        (record, exception) -> {	
		        	// 에러 핸들러에서 지정한 재시도만큼 모두 최종 실패, 이곳이 실행됨. 이곳에서 어느 토픽으로 보낼지를 리턴해야함.
		        	log.info("run recoverer. original record: {}, exception: {}", record, exception);
		        	// DLQ 용도의 토픽을 만들어서 리턴함. 보통 .dlt 를 suffix 로 하고, record.partition 즉 동일 파티션으로 지정하나 
		        	// 통일성있게 규칙을 만들어서 따르면 됨.
		            return new TopicPartition(record.topic() + ".dlt", 0);	            
		        });		
		
		factory.setRetryTemplate(retryTemplate());
		
		// 아래 setRecoveryCallback 은 안해도 잘 동작함. 
		// retryTemplate 이 모두 실패시 콜백을 받아서 뭔가 처리하고 싶을때 구현하면 됨. 
		factory.setRecoveryCallback((context -> {
			if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
	            log.info("여기는 호출이 되지 않더군..");
			} 
			else{
				log.error("retryTemplate의 최대 재시도횟수까지 해도 실패한 경우 여기가 호출됨. setRecoveryCallback. context: " + context);
	    	
				// exception을 throw 해야지 최종적으로 commit 이 수행되어 해당 offset 을 무시하고 다음 offset을 시도함. 
				// 에러 핸들러가 동작하기 위해서는 exception throw 해야함. 그래야 DLQ로 메시지 넘어감. 
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}
			return null;
		}));
		
		// retry template 를 반복하는 횟수. 1인 경우 (1)+1=2회,  3인 경우 (1)+3 = 4회, 
		// 1000L은 retry template 간 interval. 
		ErrorHandler errorHandler = new SeekToCurrentErrorHandler(recoverer, new FixedBackOff(1000L, 1L));
		factory.setErrorHandler(errorHandler);
	        
		ContainerProperties properties = factory.getContainerProperties();
		properties.setAckMode(AckMode.MANUAL);		// 실무에서는 auto commit 대신 매뉴얼 commit을 이용.
		
		return factory;
	}
	
	private RetryTemplate retryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();
		 
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(500L);  // 500ms 쉬었다가 ...
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
 
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(5);			// 최대 5번까지 재시도
        retryTemplate.setRetryPolicy(retryPolicy);
 
        return retryTemplate;
    }
}
