package jdtx.repl.main.api.struct;

import jandcode.dbm.db.*;

public interface IJdxDbStructReader {

    void setDb(Db db);

    /**
     * ������ �� �� �� ���������
     */
    IJdxDbStruct readDbStruct() throws Exception;

}
