package jdtx.repl.main.api.util;

public class UtDbNameManager_Firebird extends UtDbNameManager_Custom {

    public UtDbNameManager_Firebird() {
        MAX_LEN = 31;
        HASH_SUFFIX_LEN = 8;
    }

}
