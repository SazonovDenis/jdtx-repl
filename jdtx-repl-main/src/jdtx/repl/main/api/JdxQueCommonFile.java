package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.io.*;

import java.io.*;

/**
 *
 */
public class JdxQueCommonFile implements IJdxQueCommon {

    private long queType;

    private String baseDir;

    private Db db;
    //private DbUtils ut;

    public JdxQueCommonFile(Db db, long queType) throws Exception {
        this.db = db;
        this.queType = queType;
        // чтение структуры
        //IJdxDbStructReader reader = new JdxDbStructReader();
        //reader.setDb(db);
        //IJdxDbStruct struct = reader.readDbStruct();
        //
        //ut = new DbUtils(db, struct);
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
        UtFile.mkdirs(baseDir);
    }

    public long put(IReplica replica) throws Exception {
        //
        if (queType == -1) {
            throw new XError("invalid queType");
        }
        //
        long queMaxNo = getMaxNo();
        long queNextNo = queMaxNo + 1;
        if (replica.getNo() != -1 && replica.getNo() != queNextNo) {
            throw new XError("invalid no: replica.getNo = " + replica.getNo() + ", queMaxNo = " + queMaxNo);
        }

        //
        String actualFileName = genFileName(queNextNo);
        File actualFile = new File(baseDir + actualFileName);
        if (replica.getFile().getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            FileUtils.copyFile(replica.getFile(), actualFile);
        }

        //
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que (id, que_type, db_id, age) values (:id, :que_type, :db_id, :age)";
        db.execSql(sql, UtCnv.toMap(
                "id", queNextNo,
                "que_type", queType,
                "db_id", replica.getDbId(),
                "age", replica.getAge()
        ));

        //
        if (replica.getNo() == -1) {
            replica.setNo(queNextNo);
        }

        //
        return queNextNo;
    }

    public IReplica getByNo(long no) {
        String actualFileName = genFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        IReplica replica = new ReplicaFile();
        replica.setNo(no);
        replica.setFile(actualFile);
        return replica;
    }

    public long getMaxNo() throws Exception {
        String sql = "select max(id) as maxNo, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxNo");
        }
    }

    String genFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

}
