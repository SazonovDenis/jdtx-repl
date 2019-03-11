package jdtx.repl.main.api;

import jandcode.dbm.data.DataRecord;
import jandcode.dbm.db.Db;
import jandcode.utils.UtCnv;
import jandcode.utils.UtFile;
import jandcode.utils.UtString;
import jandcode.utils.error.XError;
import jdtx.repl.main.api.struct.IJdxDbStruct;
import jdtx.repl.main.api.struct.IJdxDbStructReader;
import jdtx.repl.main.api.struct.JdxDbStructReader;
import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Формирователь очереди реплик.
 * Физическая реализация хранения реплик и их упорядочивание.
 */
public class JdxQuePersonalFile implements IJdxQuePersonal {

    private String queType;

    private String baseDir;

    private Db db;
    private DbUtils ut;

    public JdxQuePersonalFile(Db db, int queType) throws Exception {
        if (queType == -1) {
            throw new XError("invalid queType");
        }
        //
        this.db = db;
        this.queType = JdxQueType.table_suffix[queType];
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
        if (baseDir == null || baseDir.length() == 0) {
            throw new XError("Invalid baseDir");
        }
        //
        this.baseDir = UtFile.unnormPath(baseDir) + "/";
        //
        UtFile.mkdirs(baseDir);
    }


    public void put(IReplica replica) throws Exception {
        // Проверки: правильность номера реплики
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        // Проверки: правильность очередности реплик
        long queMaxAge = getMaxAge();
        if (replica.getAge() != queMaxAge + 1) {
            throw new XError("invalid replica.getAge: " + replica.getAge() + ", que.age: " + queMaxAge);
        }

        // Помещаем в очередь
        String actualFileName = genFileName(replica.getAge());
        File actualFile = new File(baseDir + actualFileName);
        if (replica.getFile().getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            FileUtils.moveFile(replica.getFile(), actualFile);
        }
        //
        long id = ut.getNextGenerator(JdxUtils.sys_gen_prefix + "que" + queType);
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que" + queType + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "ws_id", replica.getWsId(),
                "age", replica.getAge(),
                "replica_type", replica.getReplicaType()
        ));
    }

    public long getMaxAge() throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que" + queType;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
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
