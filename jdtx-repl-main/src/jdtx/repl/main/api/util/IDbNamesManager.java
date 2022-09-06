package jdtx.repl.main.api.util;

public interface IDbNamesManager {

    String getShortName(String name, int additionalLen);

    String getShortName(String name, String prefix);

    String getShortName(String name, String prefix, String suffix);

}
