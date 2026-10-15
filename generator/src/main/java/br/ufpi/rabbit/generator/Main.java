package br.ufpi.rabbit.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Container 1 - Gerador de mensagens.
 * Publica avaliações de produtos (review.product) e solicitações de suporte (support.request)
 * no Topic Exchange, a MESSAGES_PER_SECOND msg/s (mínimo 5).
 */
public class Main {

    static final String[] PRODUTOS = {"fone de ouvido", "notebook", "smartphone", "cafeteira", "mouse sem fio",
            "teclado mecânico", "caixa de som", "monitor", "mochila", "relógio inteligente"};

    static final String[] POSITIVAS = {
            "Adorei! Qualidade ótima e muito bom acabamento.",
            "Super recomendo, superou todas as minhas expectativas.",
            "Entrega rápida e produto perfeito, estou muito satisfeito.",
            "Maravilhoso, vale cada centavo investido.",
            "Excelente custo benefício, compra perfeita.",
            "Amei, material de ótima qualidade e funciona muito bem."};

    static final String[] NEGATIVAS = {
            "Péssimo, quebrou logo na primeira semana de uso.",
            "Horrível, não recomendo, produto veio com defeito.",
            "Muito decepcionante, a qualidade é ruim e não funciona direito.",
            "Arrependido da compra, parou de funcionar rápido.",
            "Pior experiência, chegou quebrado e a entrega atrasou.",
            "Não gostei, produto frágil e totalmente diferente do anunciado."};

    static final String[] NEUTRAS = {
            "Produto razoável, atende o básico e nada de especial.",
            "Chegou dentro do prazo, é um produto normal e mediano.",
            "Funciona como descrito, uso comum sem surpresas.",
            "Item padrão, cumpre o que promete, nada demais.",
            "Mediano, serve para o dia a dia, igual aos outros do mercado.",
            "Recebi conforme o anúncio, ainda estou testando e parece ok."};

    static final String[] ACESSO = {
            "Não consigo entrar na minha conta, aparece senha incorreta.",
            "Meu login foi bloqueado e o código de verificação não chega.",
            "Acesso negado ao módulo de vendas, preciso de permissão.",
            "Esqueci a senha e o link de redefinição não funciona.",
            "Conta suspensa, não consigo acessar o sistema."};

    static final String[] DESEMPENHO = {
            "O sistema está extremamente lento ao abrir relatórios.",
            "O aplicativo trava e fica pesado quando uso planilhas grandes.",
            "As páginas demoram muito para carregar no horário de pico.",
            "A tela congela ao exportar dados, muita lentidão.",
            "Alto consumo de memória e cpu deixa tudo lento."};

    static final String[] CONEXAO = {
            "Erro de conexão: o servidor não responde.",
            "A conexão cai toda hora e perde a sincronização.",
            "Timeout ao conectar com a vpn da empresa.",
            "Sem internet no aplicativo, falha de rede ao sincronizar.",
            "Conexão recusada ao conectar no banco de dados remoto."};

    static final Random RND = new Random();

    public static void main(String[] args) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(env("RABBITMQ_HOST", "rabbitmq"));
        f.setPort(Integer.parseInt(env("RABBITMQ_PORT", "5672")));
        f.setUsername(env("RABBITMQ_USER", "guest"));
        f.setPassword(env("RABBITMQ_PASS", "guest"));
        f.setAutomaticRecoveryEnabled(true);

        int rate = Math.max(5, Integer.parseInt(env("MESSAGES_PER_SECOND", "10")));
        int errorEvery = Integer.parseInt(env("ERROR_EVERY", "100")); // 0 = nunca gera mensagem inválida

        Connection conn = connect(f);
        Channel ch = conn.createChannel();
        Topology.declare(ch);

        ObjectMapper om = new ObjectMapper();
        long[] counter = {0};

        Runnable task = () -> {
            try {
                long n = ++counter[0];
                boolean review = RND.nextBoolean();
                String routingKey = review ? Topology.RK_REVIEW : Topology.RK_SUPPORT;

                byte[] body;
                String id = UUID.randomUUID().toString();
                if (errorEvery > 0 && n % errorEvery == 0) {
                    // Mensagem propositalmente inválida (não é JSON) -> o consumidor falha -> DLQ
                    body = ("mensagem-corrompida-" + n).getBytes(StandardCharsets.UTF_8);
                    System.out.printf("[GERADOR] #%d %s -> %s (MENSAGEM INVÁLIDA, vai para a DLQ)%n", n, id, routingKey);
                } else {
                    Map<String, Object> msg = new LinkedHashMap<>();
                    msg.put("id", id);
                    msg.put("timestamp", Instant.now().toString());
                    msg.put("type", review ? "review" : "support");
                    msg.put("content", review ? novaAvaliacao() : novaSolicitacao(n));
                    body = om.writeValueAsBytes(msg);
                    System.out.printf("[GERADOR] #%d %s -> %s | %s%n", n, id, routingKey, msg.get("content"));
                }

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .deliveryMode(2) // persistente
                        .messageId(id)
                        .type(routingKey)
                        .timestamp(new Date())
                        .build();
                ch.basicPublish(Topology.EXCHANGE, routingKey, props, body);
            } catch (Exception e) {
                System.err.println("[GERADOR] falha ao publicar: " + e);
            }
        };

        System.out.println("[GERADOR] iniciando: " + rate + " msg/s");
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        ex.scheduleAtFixedRate(task, 0, 1_000_000_000L / rate, TimeUnit.NANOSECONDS);
    }

    static String novaAvaliacao() {
        String[][] grupos = {POSITIVAS, NEGATIVAS, NEUTRAS};
        String[] g = grupos[RND.nextInt(grupos.length)];
        return "Comprei " + pick(PRODUTOS) + ". " + pick(g);
    }

    static String novaSolicitacao(long n) {
        String[][] grupos = {ACESSO, DESEMPENHO, CONEXAO};
        String[] g = grupos[RND.nextInt(grupos.length)];
        return "Chamado " + n + ": " + pick(g);
    }

    static String pick(String[] a) { return a[RND.nextInt(a.length)]; }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    static Connection connect(ConnectionFactory f) throws InterruptedException {
        while (true) {
            try {
                return f.newConnection("generator");
            } catch (Exception e) {
                System.out.println("[GERADOR] aguardando RabbitMQ... (" + e.getMessage() + ")");
                Thread.sleep(3000);
            }
        }
    }
}
