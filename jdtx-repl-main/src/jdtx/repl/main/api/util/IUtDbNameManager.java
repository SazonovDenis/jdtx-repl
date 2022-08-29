package jdtx.repl.main.api.util;

public interface IUtDbNameManager {

    String getShortName(String name, int additionalLen);

    String getShortName(String name, String prefix);

    String getShortName(String name, String prefix, String suffix);

}
