package producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Писатель сообщений
 */
public class MyProducer implements Runnable {

    private final String numberProducer = UUID.randomUUID().toString();

    /**
     * Канал с которым работает писатель.
     */
    private final Channel channel;

    /**
     * exchange для писателя
     */
    private final String exchange;

    private final Set<Pair> pairs;

    public MyProducer(Channel channel, String exchange, Set<Pair> pairs) {
        this.channel = channel;
        this.exchange = exchange;
        this.pairs = pairs;

        try {
            channel.exchangeDeclare(exchange, "direct");

            for (Pair pair : pairs) {
                final String queue = pair.getLeft().toString();
                final String routingKey = pair.getRight().toString();

                channel.queueDeclare(queue, true, false, false, null);
                channel.queueBind(queue, exchange, routingKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод будет эмулировать работу писателя.
     * Каждые N случайных секунд, будет писать сообщение.
     */
    @Override
    public void run() {
        while (true) {
            try {
                for (Pair pair : pairs) {
                    final String queue = pair.getLeft().toString();
                    final String routingKey = pair.getRight().toString();

                    final String buffer = "[" + numberProducer + " - " + exchange + " - " + queue + ":" + routingKey + "] " + System.currentTimeMillis() / 1000L;

                    if (channel.isOpen()) {
                        try {
                            channel.basicPublish(exchange, routingKey, MessageProperties.TEXT_PLAIN, buffer.getBytes());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    Thread.sleep(new Random().nextInt(1000) + 1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();

                break;
            }
        }
    }

}
