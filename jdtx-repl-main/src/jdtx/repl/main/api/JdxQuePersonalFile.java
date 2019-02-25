package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;

import java.io.*;

/**
 * Формирователь очереди реплик.
 * Физическая реализация хранения реплик и их упорядочивание.
 */
public class JdxQuePersonalFile implements IJdxQuePersonal {

    private long queType;

    private String baseDir;

    private Db db;
    private DbUtils ut;

    public JdxQuePersonalFile(Db db, long queType) throws Exception {
        this.db = db;
        this.queType = queType;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        //
        ut = new DbUtils(db, struct);
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
        UtFile.mkdirs(baseDir);
    }


    public void put(IReplica replica) throws Exception {
        //
        if (queType == -1) {
            throw new XError("invalid queType");
        }
        //
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        //
        long queMaxAge = getMaxAge();
        if (queMaxAge != -1 && replica.getAge() != queMaxAge + 1) {
            throw new XError("invalid age: replica.getAge = " + replica.getAge() + ", queMaxAge = " + queMaxAge);
        }

        //
        //long id_temp = ut.getCurrId(JdxUtils.sys_gen_prefix + "que");
        String actualFileName = genFileName(replica.getAge());
        File actualFile = new File(baseDir + actualFileName);
        FileUtils.copyFile(replica.getFile(), actualFile);

        //
        long id = ut.getNextGenerator(JdxUtils.sys_gen_prefix + "que");
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que (id, que_type, ws_id, age) values (:id, :que_type, :ws_id, :age)";
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "que_type", queType,
                "ws_id", replica.getWsId(),
                "age", replica.getAge()
        ));
    }

    public long getMaxAge() throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }

    public IReplica getByAge(long age) throws Exception {
        String actualFileName = genFileName(age);
        File actualFile = new File(baseDir + actualFileName);
        IReplica replica = new ReplicaFile();
        replica.setAge(age);
        replica.setFile(actualFile);
        return replica;
    }


    String genFileName(long age) {
        return UtString.padLeft(String.valueOf(age), 9, '0') + ".xml";
    }

}
