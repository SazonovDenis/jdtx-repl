package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;

public interface ISqlScriptExecutor {

    void exec(Db db) throws Exception;

}
