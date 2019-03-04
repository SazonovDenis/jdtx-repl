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

    private String queType;

    private String baseDir;

    private Db db;

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
        // Проверки: правильность возраста реплики
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        // Проверки: правильность кода рабочей станции
        if (replica.getWsId() <= 0) {
            throw new XError("invalid replica.wsId");
        }
        // Проверки: правильность очередности реплик
        long queMaxAge = getMaxAge(replica.getWsId());
        if (replica.getAge() != queMaxAge + 1) {
            throw new XError("invalid replica.age: " + replica.getAge() + ", que.age: " + queMaxAge);
        }

        // Генерим следующий номер
        long queNextNo = getMaxNo() + 1;

        // Помещаем в очередь
        String actualFileName = genFileName(queNextNo);
        File actualFile = new File(baseDir + actualFileName);
        if (replica.getFile().getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            FileUtils.moveFile(replica.getFile(), actualFile);
        }

        //
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que" + queType + " (id, ws_id, age) values (:id, :ws_id, :age)";
        db.execSql(sql, UtCnv.toMap(
                "id", queNextNo,
                "ws_id", replica.getWsId(),
                "age", replica.getAge()
        ));

        //
        return queNextNo;
    }

    public IReplica getByNo(long no) {
        String actualFileName = genFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        IReplica replica = new ReplicaFile();
        replica.setFile(actualFile);
        //
        if (!actualFile.exists()) {
            throw new XError("Replica file is not exists: " + actualFile.getAbsolutePath());
        }
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
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

}
