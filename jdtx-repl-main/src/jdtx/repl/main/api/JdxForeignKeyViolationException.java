package jdtx.repl.main.api;

import java.util.*;

class JdxForeignKeyViolationException extends Exception {

    Map recValues;
    Map recParams;

    JdxForeignKeyViolationException(Exception e){
        super(e);
    }

}
