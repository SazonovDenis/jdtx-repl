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
public class JdxQueCreatorFile implements IJdxQueOut {

    long queType;

    String baseDir;

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
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        //
        long queMaxAge = getMaxAge();
        if (replica.getAge() != queMaxAge + 1) {
            throw new XError("invalid age: replica.getAge = " + replica.getAge() + ", queMaxAge = " + queMaxAge);
        }

        //
        //long id_temp = ut.getCurrId(JdxUtils.sys_gen_prefix + "que");
        String actualFileName = genFileName(replica.getAge());
        File actualFile = new File(baseDir + actualFileName);
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

    public long getMaxAge() throws Exception {
        String sql = "select max(age) as age from " + JdxUtils.sys_table_prefix + "que where que_type = " + queType;
        return db.loadSql(sql).getCurRec().getValueLong("age");
    }


    String genFileName(long age) {
        return UtString.padLeft(String.valueOf(age), 9, '0') + ".xml";
    }

}
