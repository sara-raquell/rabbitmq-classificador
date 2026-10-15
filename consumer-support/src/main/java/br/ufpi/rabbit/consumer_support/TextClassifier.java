package br.ufpi.rabbit.consumer_support;

import smile.classification.LogisticRegression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IA embarcada: regressão logística multinomial (Smile) sobre bag-of-words binário.
 * O modelo é treinado na inicialização do container a partir de um CSV "rótulo;texto" em resources.
 */
final class TextClassifier {
    private final Map<String, Integer> vocab;
    private final String[] labels;
    private final LogisticRegression model;

    private TextClassifier(Map<String, Integer> vocab, String[] labels, LogisticRegression model) {
        this.vocab = vocab;
        this.labels = labels;
        this.model = model;
    }

    static TextClassifier trainFromResource(String resource) throws IOException {
        List<String> texts = new ArrayList<>();
        List<String> rotulos = new ArrayList<>();
        try (InputStream in = TextClassifier.class.getResourceAsStream(resource);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                int i = line.indexOf(';');
                rotulos.add(line.substring(0, i).trim());
                texts.add(line.substring(i + 1).trim());
            }
        }

        Set<String> classes = new LinkedHashSet<>(rotulos);
        String[] labels = classes.toArray(new String[0]);
        Map<String, Integer> labelIdx = new HashMap<>();
        for (int i = 0; i < labels.length; i++) labelIdx.put(labels[i], i);

        Map<String, Integer> vocab = new HashMap<>();
        for (String t : texts) {
            for (String tok : tokenize(t)) vocab.putIfAbsent(tok, vocab.size());
        }

        double[][] x = new double[texts.size()][];
        int[] y = new int[texts.size()];
        for (int i = 0; i < texts.size(); i++) {
            x[i] = vectorize(vocab, texts.get(i));
            y[i] = labelIdx.get(rotulos.get(i));
        }

        LogisticRegression model = LogisticRegression.fit(x, y);
        System.out.printf("[IA] modelo treinado: %d exemplos, %d termos, classes=%s%n",
                texts.size(), vocab.size(), String.join(" | ", labels));
        return new TextClassifier(vocab, labels, model);
    }

    String classify(String text) {
        return labels[model.predict(vectorize(vocab, text))];
    }

    private static double[] vectorize(Map<String, Integer> vocab, String text) {
        double[] v = new double[vocab.size()];
        for (String tok : tokenize(text)) {
            Integer idx = vocab.get(tok);
            if (idx != null) v[idx] = 1.0;
        }
        return v;
    }

    private static List<String> tokenize(String text) {
        String s = Normalizer.normalize(text.toLowerCase(), Normalizer.Form.NFD).replaceAll("\\p{M}", "");
        List<String> out = new ArrayList<>();
        for (String tok : s.split("[^a-z]+")) {
            if (tok.length() >= 3) out.add(tok);
        }
        return out;
    }
}
