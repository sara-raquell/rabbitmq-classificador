package br.ufpi.rabbit.consumer_support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * Container 4 - Consumidor 2: classificação de solicitações de suporte (acesso / desempenho / conexão).
 * - consome a fila Q_SUPPORT com manual ACK;
 * - processa devagar (PROCESSING_DELAY_MS) para as filas crescerem;
 * - em caso de erro faz basicNack(requeue=false) -> mensagem vai para a DLQ.
 */
public class Main {

    static final String QUEUE = Topology.Q_SUPPORT;
    static final String EXPECTED_TYPE = "support";

    public static void main(String[] args) throws Exception {
        long delayMs = Long.parseLong(env("PROCESSING_DELAY_MS", "1000"));
        int prefetch = Integer.parseInt(env("PREFETCH", "1"));

        TextClassifier classifier = TextClassifier.trainFromResource("/treino_suporte.csv");

        ConnectionFactory f = new ConnectionFactory();
        f.setHost(env("RABBITMQ_HOST", "rabbitmq"));
        f.setPort(Integer.parseInt(env("RABBITMQ_PORT", "5672")));
        f.setUsername(env("RABBITMQ_USER", "guest"));
        f.setPassword(env("RABBITMQ_PASS", "guest"));
        f.setAutomaticRecoveryEnabled(true);

        Connection conn = connect(f);
        Channel ch = conn.createChannel();
        Topology.declare(ch);
        ch.basicQos(prefetch);

        ObjectMapper om = new ObjectMapper();

        DeliverCallback callback = (consumerTag, delivery) -> {
            long tag = delivery.getEnvelope().getDeliveryTag();
            try {
                JsonNode n = om.readTree(delivery.getBody()); // lança exceção se não for JSON válido
                for (String campo : new String[]{"id", "timestamp", "type", "content"}) {
                    if (!n.hasNonNull(campo)) throw new IllegalArgumentException("campo ausente: " + campo);
                }
                if (!EXPECTED_TYPE.equals(n.get("type").asText())) {
                    throw new IllegalArgumentException("tipo inesperado: " + n.get("type").asText());
                }

                String content = n.get("content").asText();
                String resultado = classifier.classify(content);

                Thread.sleep(delayMs); // processamento lento de propósito

                System.out.printf("[SUPORTE] %s | gerada em %s | \"%s\" => %s%n",
                        n.get("id").asText(), n.get("timestamp").asText(), content, resultado.toUpperCase());
                ch.basicAck(tag, false);
            } catch (Exception e) {
                System.err.printf("[SUPORTE] ERRO ao processar (%s) -> enviando para a DLQ%n", e.getMessage());
                try {
                    ch.basicNack(tag, false, false); // requeue=false => Dead Letter Exchange
                } catch (IOException io) {
                    System.err.println("[SUPORTE] falha ao enviar NACK: " + io);
                }
            }
        };

        ch.basicConsume(QUEUE, false, callback, tag -> { });
        System.out.println("[SUPORTE] aguardando mensagens em " + QUEUE + " (delay=" + delayMs + "ms, prefetch=" + prefetch + ")");
        Thread.currentThread().join();
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    static Connection connect(ConnectionFactory f) throws InterruptedException {
        while (true) {
            try {
                return f.newConnection("consumer-support");
            } catch (Exception e) {
                System.out.println("[SUPORTE] aguardando RabbitMQ... (" + e.getMessage() + ")");
                Thread.sleep(3000);
            }
        }
    }
}
