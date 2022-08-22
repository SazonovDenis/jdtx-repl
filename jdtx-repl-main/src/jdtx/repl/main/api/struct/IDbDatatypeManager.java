package jdtx.repl.main.api.struct;

import jandcode.dbm.db.*;

public interface IDbDatatypeManager {

    void setDb(Db db) throws Exception;

    JdxDataType dbDatatypeToJdxDatatype(String dbDataType) throws Exception;

}
