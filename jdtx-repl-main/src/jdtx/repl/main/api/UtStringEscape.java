package jdtx.repl.main.api;

import java.io.*;
import java.util.*;

/**
 * Маскировка спецсимволов
 */
public class UtStringEscape {

    public static String escapeJava(String str) {
        StringWriter ioe = new StringWriter(str.length() * 2);
        try {
            escapeJavaStyleString(ioe, str);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ioe.toString();
    }

    private static void escapeJavaStyleString(Writer out, String str) throws IOException {
        if (str == null) {
            return;
        }

        int sz = str.length();
        for (int i = 0; i < sz; ++i) {
            char ch = str.charAt(i);

            if (ch < 32) {
                switch (ch) {
                    case '\b':
                        out.write(92);
                        out.write(98);
                        break;
                    case '\t':
                        out.write(92);
                        out.write(116);
                        break;
                    case '\n':
                        out.write(92);
                        out.write(110);
                        break;
                    case '\u000b':
                    default:
                        if (ch > 15) {
                            out.write("\\u00" + hex(ch));
                        } else {
                            out.write("\\u000" + hex(ch));
                        }
                        break;
                    case '\f':
                        out.write(92);
                        out.write(102);
                        break;
                    case '\r':
                        out.write(92);
                        out.write(114);
                }
            } else {
                switch (ch) {
                    case '\"':
                        out.write(92);
                        out.write(34);
                        break;
                    case '\\':
                        out.write(92);
                        out.write(92);
                        break;
                    default:
                        out.write(ch);
                }
            }
        }
    }

    private static String hex(char ch) {
        return Integer.toHexString(ch).toUpperCase(Locale.ENGLISH);
    }

}
