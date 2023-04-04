package com.github.kafsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseTest {

    protected PulsarAdmin pulsarAdmin;

    protected PulsarClient pulsarClient;

    public void init() throws PulsarClientException {
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PulsarConst.HTTP_URL).build();
        pulsarClient = PulsarClient.builder().serviceUrl(PulsarConst.TCP_URL).build();
    }

    public void close() throws PulsarClientException {
        pulsarAdmin.close();
        pulsarClient.close();
    }

}
