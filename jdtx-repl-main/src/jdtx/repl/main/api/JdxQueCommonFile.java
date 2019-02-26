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
        // Проверки правильности номера реплики
        if (replica.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        //
        if (replica.getWsId() <= 0) {
            throw new XError("invalid replica.wsId");
        }
        //
        long queMaxAge = getMaxAge(replica.getWsId());
        if (replica.getAge() == 0) {
            // Установочная (самая первая, полная) реплика
            if (queMaxAge != -1) {
                throw new XError("Setup replica is not allowed in que.age: " + queMaxAge);
            }
        } else {
            // Очередная реплика
            if (replica.getAge() != queMaxAge + 1) {
                throw new XError("invalid replica.age: " + replica.getAge() + ", que.age: " + queMaxAge);
            }
        }

        // Генерим следующий номер
        long queNextNo = getMaxNo() + 1;

        // Помещаем в очередь
        String actualFileName = genFileName(queNextNo);
        File actualFile = new File(baseDir + actualFileName);
        if (replica.getFile().getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            // Иногда две очереди разделяют одно место хранения,
            // тогда файл копировать не нужно
            // todo - может помешать процедурам удаления реплик
            FileUtils.copyFile(replica.getFile(), actualFile);
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
        return replica;
    }

    public long getMaxAge(long wsId) throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que" + queType + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }

    public long getMaxNo() throws Exception {
        String sql = "select max(id) as maxNo, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que" + queType;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
        } else {
            return rec.getValueLong("maxNo");
        }
    }

    String genFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

}
