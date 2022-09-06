package jdtx.repl.main.api.util;

import jandcode.utils.*;

public class DbNamesManager_Custom implements IDbNamesManager {

    int MAX_LEN = 10;
    int HASH_SUFFIX_LEN = 2;

    @Override
    public String getShortName(String name, int prefixLen) {
        if ((name.length() + prefixLen) <= MAX_LEN) {
            return name;
        }
        //
        String md5 = UtString.md5Str(name);
        return name.substring(0, MAX_LEN - HASH_SUFFIX_LEN - prefixLen) + md5.substring(0, HASH_SUFFIX_LEN);
    }

    @Override
    public String getShortName(String name, String prefix) {
        return prefix + getShortName(name, prefix.length());
    }

    @Override
    public String getShortName(String name, String prefix, String suffix) {
        return prefix + getShortName(name, prefix.length() + suffix.length()) + suffix;
    }


}
