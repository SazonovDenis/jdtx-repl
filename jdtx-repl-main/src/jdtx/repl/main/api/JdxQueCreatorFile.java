package jdtx.repl.main.api;

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
public class JdxQueCreatorFile implements IJdxQue {

    long queType;

    String baseFilePath;

    Db db;
    DbUtils ut;

    public JdxQueCreatorFile(Db db) throws Exception {
        this.db = db;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        //
        ut = new DbUtils(db, struct);
    }


    public void put(IReplica replica) throws Exception {
        //
        if (queType == -1) {
            throw new XError("invalid queType");
        }
        //
        long queMaxAge = getMaxAge();
        if (replica.getAge() != queMaxAge + 1) {
            throw new XError("invalid age: replica.getAge = " + replica.getAge() + ", queMaxAge = " + queMaxAge);
        }
        //
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }

        //
        long id_temp = ut.getCurrId(JdxUtils.sys_gen_prefix + "que");
        String actualFileName = genFileName(id_temp);
        File actualFile = new File(baseFilePath + actualFileName);
        FileUtils.copyFile(replica.getFile(), actualFile);

        //
        long id = ut.genId(JdxUtils.sys_gen_prefix + "que");
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que (id, que_type, age) values (:id, :que_type, :age)";
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "que_type", queType,
                "age", replica.getAge()
        ));

    }

    IReplica getById(long id) {
        String actualFileName = genFileName(id);
        File actualFile = new File(baseFilePath + actualFileName);
        IReplica replica = new ReplicaFile();
        replica.setFile(actualFile);
        return replica;
    }

    long getMaxAge() throws Exception {
        String sql = "select max(age) as age from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        return db.loadSql(sql).getCurRec().getValueLong("age");
    }

    public long getMaxId() throws Exception {
        String sql = "select max(id) as id from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        return db.loadSql(sql).getCurRec().getValueLong("id");
    }

    String genFileName(long id) {
        return UtString.padLeft(String.valueOf(id), 9, '0') + ".xml";
    }

}
