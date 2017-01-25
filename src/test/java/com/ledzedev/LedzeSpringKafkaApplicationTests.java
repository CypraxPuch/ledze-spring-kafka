package com.ledzedev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LedzeSpringKafkaApplicationTests {
    private static Logger logger = LoggerFactory.getLogger(LedzeSpringKafkaApplicationTests.class);

    @Test
    public void pruebaReceptorDeMensajes() throws Exception {
        logger.info("\n\n\n");
        logger.info("Inicia contenedor de mensajes...");

        ContainerProperties containerProps = new ContainerProperties("topicoLedze", "topic2");
        containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
            logger.info("recibido: " + message);
        });

        KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
        container.setBeanName("testAuto");
        container.start();
        //container.stop();
    }

    @Test
    public void pruebaEnviaMensajes(){
        logger.info("\n\n\n");
        logger.info("Inicia el envío de mensajes...");

        KafkaTemplate<Integer, String> template = createTemplate();
        template.setDefaultTopic("topicoLedze");

        ListenableFuture<SendResult<Integer, String>> future1 = template.sendDefault(0, "foo");
        this.addListenableFutureCallback(future1);

        ListenableFuture<SendResult<Integer, String>> future2 = template.sendDefault(2, "bar");
        this.addListenableFutureCallback(future2);

        ListenableFuture<SendResult<Integer, String>> future3 = template.sendDefault(0, "baz");
        this.addListenableFutureCallback(future3);

        ListenableFuture<SendResult<Integer, String>> future4 = template.sendDefault(2, "qux");
        this.addListenableFutureCallback(future4);

        template.flush();

    }

    private void addListenableFutureCallback(ListenableFuture<SendResult<Integer, String>> future){
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable motivoFalla) {
                logger.info("EL ENVÍO DEL MENSAJE FALLÓ.", motivoFalla);
            }
            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                logger.info("ENVÍO DE MENSAJE EXITOSO! Mensaje=" + integerStringSendResult.getProducerRecord().value());
            }
        });
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(ContainerProperties containerProps) {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
        KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private KafkaTemplate<Integer, String> createTemplate() {
        Map<String, Object> senderProps = senderProps();
        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        return template;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupoLedze");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
