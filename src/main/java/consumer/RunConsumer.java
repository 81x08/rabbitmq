package consumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static config.Config.QUEUE_PREFIX;

public class RunConsumer {

    public static void main(String[] args) throws Exception {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri("amqp://test:test@localhost");

        final Connection connection = connectionFactory.newConnection();

        new MyConsumer(connection.createChannel(), QUEUE_PREFIX + 1, 1);
        new MyConsumer(connection.createChannel(), QUEUE_PREFIX + 2, 1);
        new MyConsumer(connection.createChannel(), QUEUE_PREFIX + 3, 1);
    }

}