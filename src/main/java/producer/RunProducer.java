package producer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static config.Config.*;

public class RunProducer {

    private static final int PRODUCERS = 3;

    public static void main(String[] args) throws Exception {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri("amqp://test:test@localhost");

        final Connection connection = connectionFactory.newConnection();

        final ExecutorService executorServiceProducer = Executors.newFixedThreadPool(PRODUCERS);

        final List<MyProducer> producers = new ArrayList<>();

        final Set<Pair> firstPair = new HashSet<>();

        firstPair.add(Pair.of(QUEUE_PREFIX + 1, ROUTING_KEY_PREFIX + 1));
        firstPair.add(Pair.of(QUEUE_PREFIX + 2, ROUTING_KEY_PREFIX + 2));

        producers.add(
                new MyProducer(
                        connection.createChannel(),
                        EXCHANGE_PREFIX + 1,
                        firstPair
                )
        );

        final Set<Pair> secondPair = new HashSet<>();

        secondPair.add(Pair.of(QUEUE_PREFIX + 3, ROUTING_KEY_PREFIX + 3));

        producers.add(
                new MyProducer(
                        connection.createChannel(),
                        EXCHANGE_PREFIX + 2,
                        secondPair
                )
        );

        producers.forEach(executorServiceProducer::submit);
    }

}
