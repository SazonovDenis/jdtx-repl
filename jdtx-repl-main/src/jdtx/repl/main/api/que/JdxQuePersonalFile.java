package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Формирователь очереди исходящих реплик.
 * Физическая реализация хранения реплик и их упорядочивания.
 */
public class JdxQuePersonalFile extends JdxQueFile implements IJdxQuePersonal {


    //
    private Db db;

    //
    protected static Log log = LogFactory.getLog("jdtx.QuePersonal");

    //
    public JdxQuePersonalFile(Db db, String queName, long wsId) throws Exception {
        this.db = db;
        this.queName = JdxQueName.getTableSuffix(queName);
    }

    public long push(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        JdxUtils.validateReplicaFields(replica);

        // Проверки: правильность очередности реплик по возрасту age
        long queMaxAge = getMaxAge();
        if (replica.getInfo().getAge() != queMaxAge + 1) {
            throw new XError("Invalid replica.getReplicaInfo().getAge: " + replica.getInfo().getAge() + ", que.age: " + queMaxAge);
        }

        // Номер по возрасту реплики
        long queNo = replica.getInfo().getAge();

        // Помещаем файл на место хранения файлов очереди.
        //if (replica.getFile() != null) {
            put(replica, queNo);
        //}

        // Отмечаем в БД
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que_" + queName + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                //"id", id,
                "id", replica.getInfo().getAge(),
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "replica_type", replica.getInfo().getReplicaType()
        ));

        //
        return queNo;
    }

    public long getMaxAge() throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que_" + queName;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
        } else {
            return rec.getValueLong("maxAge");
        }
    }

    public IReplica getByAge(long age) throws Exception {
        IReplica replica = get(age);

        //
        readReplicaInfoByAge(age, replica.getInfo());

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

    private void readReplicaInfoByAge(long age, IReplicaInfo replicaInfo) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "que_" + queName + " where age = " + age;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found: " + age);
        }
        replicaInfo.setAge(rec.getValueLong("age"));
        replicaInfo.setWsId(rec.getValueLong("ws_id"));
        replicaInfo.setReplicaType(rec.getValueInt("replica_type"));
    }


}
