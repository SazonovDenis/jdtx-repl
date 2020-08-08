package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.DbUtils;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;

import java.io.*;

/**
 * Формирователь очереди реплик.
 * Физическая реализация хранения реплик и их упорядочивание.
 */
public class JdxQuePersonalFile extends JdxQueFile implements IJdxQuePersonal {

    //
    private Db db;
    private DbUtils ut;

    //
    protected static Log log = LogFactory.getLog("jdtx.QuePersonal");


    public JdxQuePersonalFile(Db db, int queType) throws Exception {
        if (queType <= JdxQueType.NONE) {
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
    }


    public void put(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        JdxUtils.validateReplica(replica);

        // Проверки: правильность очередности реплик по возрасту age
        long queMaxAge = getMaxAge();
        if (replica.getInfo().getAge() != queMaxAge + 1) {
            throw new XError("invalid replica.getReplicaInfo().getAge: " + replica.getInfo().getAge() + ", que.age: " + queMaxAge);
        }

        // Помещаем файл на место хранения файлов очереди.
        String actualFileName = genFileName(replica.getInfo().getAge());
        File actualFile = new File(baseDir + actualFileName);
        // Если файл, указанный у реплики не совпадает с постоянным местом хранения, то файл переносим на постоянное место.
        if (replica.getFile().getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            // Если какой-то файл уже занимает постоянное место, то этот файл НЕ удаляем.
            if (actualFile.exists()) {
                throw new XError("ActualFile already exists: " + actualFile.getAbsolutePath());
            }
            // Переносим файл на постоянное место
            FileUtils.moveFile(replica.getFile(), actualFile);
        }

        // Отмечаем в БД
        long id = ut.getNextGenerator(JdxUtils.sys_gen_prefix + "que" + queType);
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que" + queType + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "replica_type", replica.getInfo().getReplicaType()
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
        IReplica replica = new ReplicaFile();

        //
        readReplicaInfoByAge(age, replica.getInfo());

        //
        String actualFileName = genFileName(age);
        File actualFile = new File(baseDir + actualFileName);
        replica.setFile(actualFile);

        //
        return replica;
    }

    public IReplicaInfo getInfoByAge(long age) throws Exception {
        IReplicaInfo replicaInfo = new ReplicaInfo();

        //
        readReplicaInfoByAge(age, replicaInfo);

        //
        return replicaInfo;
    }

    void readReplicaInfoByAge(long age, IReplicaInfo replicaInfo) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "que" + queType + " where age = " + age;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found: " + age);
        }
        replicaInfo.setAge(rec.getValueLong("age"));
        replicaInfo.setWsId(rec.getValueLong("ws_id"));
        replicaInfo.setReplicaType(rec.getValueInt("replica_type"));
    }


}
