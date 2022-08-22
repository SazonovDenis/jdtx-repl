package jdtx.repl.main.api.struct;

import jandcode.dbm.db.*;

public interface IJdxDbStructReader {

    void setDb(Db db) throws Exception;

    /**
     * Читаем из БД ее структуру
     */
    IJdxDbStruct readDbStruct() throws Exception;

}
