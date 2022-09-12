package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

public class FieldValueGenerator_String extends FieldValueGenerator {

    int maxSize = 0;

    String template;

    String alphabet = "0123456789" +
            "әғқңөұүhі" +
            "ӘҒҚҢӨҰҮHІ" +
            "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" +
            "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ" +
            "abcdefghijklmnopqrstuvwxyz" +
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public FieldValueGenerator_String(String template) {
        super();
        this.template = template;
    }

    public FieldValueGenerator_String(String template, int maxSize) {
        super();
        this.maxSize = maxSize;
        this.template = template;
    }

    public FieldValueGenerator_String(String template, String alphabet) {
        super();
        this.template = template;
        this.alphabet = alphabet;
    }

    public FieldValueGenerator_String(String template, String alphabet, int maxSize) {
        super();
        this.maxSize = maxSize;
        this.template = template;
        this.alphabet = alphabet;
    }

    @Override
    public String genValue(IJdxField field) {
        StringBuilder sb = new StringBuilder();

        // Раскроем шаблон
        for (char chTemplate : String.valueOf(template).toCharArray()) {
            if (chTemplate == '*') {
                int idx = rnd.nextInt(alphabet.length());
                char ch = alphabet.charAt(idx);
                sb.append(ch);
            } else {
                sb.append(chTemplate);
            }
        }
        String res = sb.toString();

        // Ограничим длину
        if (maxSize != 0) {
            int len = rnd.nextInt(maxSize);
            while (res.getBytes().length > len) {
                res = res.substring(0, res.length() - 1);
            }
        }

        return res;
    }

}
