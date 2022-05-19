package jdtx.repl.main.api.audit;

import jdtx.repl.main.api.replica.*;

import java.util.*;

public class JdxForeignKeyViolationException extends Exception {

    public String tableName;
    public JdxOprType oprType;
    public Map<String, String> recValues;
    public Map<String, Object> recParams;

    public JdxForeignKeyViolationException(Exception e) {
        super(e);
    }

}
