package jdtx.repl.main.api.struct;

import jandcode.dbm.db.*;

public interface IJdxDbStructReader {

    void setDb(Db db);

    /**
     * читаем из БД ее структуру
     */
    IJdxDbStruct readDbStruct() throws Exception;

}
