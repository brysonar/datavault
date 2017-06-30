package org.datavaultplatform.worker.queue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class MqReceiver {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
    @Autowired
    private Receiver receiver;
    
    @Value("${queue.server}")
    private String queueServer;
    
    @Value("${queue.name}")
    private String queueName;
    
    @Value("${queue.user}")
    private String queueUser;
    
    @Value("${queue.password}")
    private String queuePassword;

    public void receive() throws IOException, InterruptedException, TimeoutException {

        ConnectionFactory factory = getConnectionFactory();
        
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);
        logger.info("Waiting for messages");
        
        QueueingConsumer consumer = new QueueingConsumer(channel);

        channel.basicQos(1);
        channel.basicConsume(queueName, false, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            
            // Note that the message body might contain keys/credentials
            logger.debug("Received " + message.length() + " bytes");
            logger.debug("Received message body '" + message + "'");
            
		    // Is the message a redelivery?
            boolean isRedeliver = false;
		    if (delivery.getEnvelope().isRedeliver()) {
		    	isRedeliver = true;
		    }
		    
		    receiver.process(message, isRedeliver);

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
        
        // Unreachable - the receiver never terminates
        // channel.close();
        // connection.close();
    }


	private ConnectionFactory getConnectionFactory() {
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(queueServer);
        factory.setUsername(queueUser);
        factory.setPassword(queuePassword);
		return factory;
	}


}