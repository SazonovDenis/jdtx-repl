package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.io.*;

/**
 *
 */
public class JdxQueCommon implements IJdxQueCommon {

    long queType;

    String baseDir;

    Db db;
    DbUtils ut;

    public JdxQueCommon(Db db) throws Exception {
        this.db = db;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        //
        ut = new DbUtils(db, struct);
    }

    public void put(IReplica replica) throws Exception {

    }

    public IReplica getById(long id) {
        String actualFileName = genFileName(id);
        File actualFile = new File(baseDir + actualFileName);
        IReplica replica = new ReplicaFile();
        replica.setFile(actualFile);
        return replica;
    }

    public long getMaxId() throws Exception {
        String sql = "select max(id) as id from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        return db.loadSql(sql).getCurRec().getValueLong("id");
    }

    String genFileName(long id) {
        return UtString.padLeft(String.valueOf(id), 9, '0') + ".xml";
    }

}
