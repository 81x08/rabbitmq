package consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyConsumer extends DefaultConsumer {

    /**
     * Канал подлежащий обработке слушателем.
     */
    private final Channel channel;

    /**
     * Очередь, сообщения которой, должны быть обработаны слушателем.
     */
    private final String queue;

    public MyConsumer(Channel channel, String queue, int prefetch) {
        super(channel);

        this.channel = channel;
        this.queue = queue;

        try {
            channel.queueDeclare(queue, true, false, false, null);

            channel.basicQos(prefetch);
            channel.basicConsume(queue, false, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        final String buffer = new String(body, StandardCharsets.UTF_8);

        System.out.println("* CONSUMER * " + buffer);

        try {
            channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (IOException e) {
            try {
                channel.basicReject(envelope.getDeliveryTag(), true);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}
