package jdtx.repl.main.api.data_filler;

import jdtx.repl.main.api.struct.*;

public class FileldValueGenerator_String extends FileldValueGenerator {

    int maxSize = 0;

    String template;

    String charSet = "0123456789" +
            "әғқңөұүhі" +
            "ӘҒҚҢӨҰҮHІ" +
            "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" +
            "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ" +
            "abcdefghijklmnopqrstuvwxyz" +
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public FileldValueGenerator_String(String template, int maxSize) {
        super();
        this.maxSize = maxSize;
        this.template = template;
    }

    public FileldValueGenerator_String(String template, String charSet) {
        super();
        this.template = template;
        this.charSet = charSet;
    }

    public FileldValueGenerator_String(String template, String charSet, int maxSize) {
        super();
        this.maxSize = maxSize;
        this.template = template;
        this.charSet = charSet;
    }

    @Override
    public String genValue(IJdxField field) {
        StringBuilder sb = new StringBuilder();

        for (char chTemplate : String.valueOf(template).toCharArray()) {
            if (chTemplate == '*') {
                int idx = rnd.nextInt(charSet.length());
                char ch = charSet.charAt(idx);
                sb.append(ch);
            } else {
                sb.append(chTemplate);
            }
        }

        String res = sb.toString();
        if (maxSize != 0) {
            int len = rnd.nextInt(maxSize);
            res = res.substring(0, len);
        }

        return res;
    }

}
