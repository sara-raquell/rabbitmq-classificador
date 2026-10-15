package br.ufpi.rabbit.consumer_support;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Declara (de forma idempotente) exchange, filas e DLQs. Mesma definição nos 3 serviços. */
final class Topology {
    static final String EXCHANGE = "messages.exchange";   // topic
    static final String DLX = "messages.dlx";             // direct (Dead Letter Exchange)

    static final String RK_REVIEW = "review.product";
    static final String RK_SUPPORT = "support.request";

    static final String Q_REVIEWS = "reviews.queue";
    static final String Q_SUPPORT = "support.queue";
    static final String DLQ_REVIEWS = "reviews.dlq";
    static final String DLQ_SUPPORT = "support.dlq";

    private Topology() {}

    static void declare(Channel ch) throws IOException {
        ch.exchangeDeclare(EXCHANGE, "topic", true);
        ch.exchangeDeclare(DLX, "direct", true);
        bind(ch, Q_REVIEWS, RK_REVIEW, DLQ_REVIEWS, "reviews.dead");
        bind(ch, Q_SUPPORT, RK_SUPPORT, DLQ_SUPPORT, "support.dead");
    }

    private static void bind(Channel ch, String queue, String routingKey, String dlq, String dlqKey) throws IOException {
        ch.queueDeclare(dlq, true, false, false, null);
        ch.queueBind(dlq, DLX, dlqKey);

        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", DLX);
        args.put("x-dead-letter-routing-key", dlqKey);
        ch.queueDeclare(queue, true, false, false, args);
        ch.queueBind(queue, EXCHANGE, routingKey);
    }
}
