package jdtx.repl.main.api.util;

/**
 * Сокращатель длинных имен объектов СУБД
 */
public interface IDbNames {

    String getShortName(String name, int additionalLen);

    String getShortName(String name, String prefix);

    String getShortName(String name, String prefix, String suffix);

}
