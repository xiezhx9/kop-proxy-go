package com.github.kop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaProducerMsgTest extends BaseTest {

    @BeforeAll
    public void beforeAll() throws PulsarClientException {
        super.init();
    }

    @Test
    @Timeout(60)
    public void produceMsgSuccess() throws Exception {
        String topic = UUID.randomUUID().toString();
        pulsarAdmin.topics().createPartitionedTopic("public/default/" + topic, 1);
        String msg = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, msg);
        producer.send(producerRecord);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            Assertions.assertEquals(msg, consumerRecord.value());
            break;
        }
    }

    @Test
    @Timeout(60)
    public void produceMsgAckAllSuccess() throws Exception {
        String topic = UUID.randomUUID().toString();
        pulsarAdmin.topics().createPartitionedTopic("public/default/" + topic, 1);
        String msg = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, msg);
        producer.send(producerRecord);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            Assertions.assertEquals(msg, consumerRecord.value());
            break;
        }
    }

    @Test
    @Timeout(60)
    public void produceMsgAck0Success() throws Exception {
        String topic = UUID.randomUUID().toString();
        pulsarAdmin.topics().createPartitionedTopic("public/default/" + topic, 1);
        String msg = UUID.randomUUID() + "1";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, msg);
        producer.send(producerRecord);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            Assertions.assertEquals(msg, consumerRecord.value());
            break;
        }
    }
}
