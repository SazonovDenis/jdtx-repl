package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;

import java.io.*;

/**
 *
 */
public class JdxQueCommonFile implements IJdxQueCommon {

    private String queType;

    private String baseDir;

    private Db db;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    public JdxQueCommonFile(Db db, int queType) throws Exception {
        if (queType <= JdxQueType.NONE) {
            throw new XError("invalid queType: " + queType);
        }
        //
        this.db = db;
        this.queType = JdxQueType.table_suffix[queType];
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

    public long put(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        JdxUtils.validateReplica(replica);

        // Проверки: правильность очередности реплик по возрасту age для рабочей станции wsId
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE || replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
            long queMaxAge = getMaxAge(replica.getInfo().getWsId());
            if (replica.getInfo().getAge() != queMaxAge + 1) {
                throw new XError("invalid replica.age: " + replica.getInfo().getAge() + ", que.age: " + queMaxAge);
            }
        }

        // Генерим следующий номер
        long queNextNo = getMaxNo() + 1;

        // Помещаем файл на место хранения файлов очереди.
        // Если file, указанный у реплики не совпадает с постоянным местом хранения, то файл переносим на постоянное место.
        // Если какой-то файл уже находится на постоянном месте, то этого самозванца сначала удаляем.
        String actualFileName = genFileName(queNextNo);
        File actualFile = new File(baseDir + actualFileName);
        File replicaFile = replica.getFile();
        if (replicaFile != null && replicaFile.getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            // Место случайно не занято?
            if (actualFile.exists()) {
                log.debug("actualFile.exists: " + actualFile.getAbsolutePath());
                actualFile.delete();
            }
            // Переносим файл на постоянное место
            FileUtils.moveFile(replicaFile, actualFile);
        }

        // Отмечаем в БД
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que" + queType + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", queNextNo,
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "replica_type", replica.getInfo().getReplicaType()
        ));

        //
        return queNextNo;
    }

    public IReplica getByNo(long no) throws Exception {
        IReplica replica = new ReplicaFile();

        //
        String sql = "select * from " + JdxUtils.sys_table_prefix + "que" + queType + " where id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found: " + no);
        }
        replica.getInfo().setAge(rec.getValueLong("age"));
        replica.getInfo().setWsId(rec.getValueLong("ws_id"));
        replica.getInfo().setReplicaType(rec.getValueInt("replica_type"));

        //
        String actualFileName = genFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        replica.setFile(actualFile);

        //
        return replica;
    }

    public long getMaxAge(long wsId) throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que" + queType + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
        } else {
            return rec.getValueLong("maxAge");
        }
    }

    public long getMaxNo() throws Exception {
        String sql = "select max(id) as maxNo, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que" + queType;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            // Номера в очередях начинаются от 1 (в отличие от возрастов, котрые могут начаться с 0)
            return 0;
        } else {
            return rec.getValueLong("maxNo");
        }
    }

    String genFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

}
