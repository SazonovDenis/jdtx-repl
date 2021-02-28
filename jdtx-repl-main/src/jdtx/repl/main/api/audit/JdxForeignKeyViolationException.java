package jdtx.repl.main.api.audit;

import java.util.*;

public class JdxForeignKeyViolationException extends Exception {

    public Map recValues;
    public Map recParams;

    public JdxForeignKeyViolationException(Exception e){
        super(e);
    }

}
