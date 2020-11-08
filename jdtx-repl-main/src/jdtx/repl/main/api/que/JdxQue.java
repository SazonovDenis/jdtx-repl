package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;


/**
 * Очередь реплик - хранилище дублируются в БД.
 * Реализация интерфейса {@link IJdxReplicaQue}
 */
public class JdxQue extends JdxStorageFile implements IJdxReplicaQue {

    //
    Db db;

    String queName;

    String wsPrefix;

    //
    public JdxQue(Db db, String queName, boolean queForEachWsAtSrv) {
        this.db = db;
        this.queName = UtQue.getTableSuffix(queName);
        if (queForEachWsAtSrv) {
            this.wsPrefix = "_ws";
        } else {
            this.wsPrefix = "";
        }
    }

    public String getQueName() {
        return queName;
    }


    /*
     * JdxStorageFile
     */

    @Override
    public IReplica get(long no) throws Exception {
        DataRecord rec = loadReplicaRec(no);

        //
        IReplica replica = super.get(no);

        //
        recToReplicaInfo(rec, replica.getInfo());

        //
        return replica;
    }


    /**
     * IJdxReplicaQue
     */

    public void validateReplica(IReplica replica) throws Exception {
        JdxUtils.validateReplicaFields(replica);
    }

    public long push(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        validateReplica(replica);

        // Генерим следующий номер - по порядку
        long queNo = getMaxNo() + 1;

        // Помещаем файл на место хранения файлов очереди.
        if (replica.getFile() != null) {
            put(replica, queNo);
        }

        // Отмечаем в БД
        db.startTran();
        try {
            String sql = "insert into " + JdxUtils.sys_table_prefix + "que_" + queName + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
            db.execSql(sql, UtCnv.toMap(
                    "id", queNo,
                    "ws_id", replica.getInfo().getWsId(),
                    "age", replica.getInfo().getAge(),
                    "replica_type", replica.getInfo().getReplicaType()
            ));

            //
            setMaxNo(queNo);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        //
        return queNo;
    }

    public long getMaxNo() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state" + wsPrefix;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state" + wsPrefix + " set que_" + queName + "_no = " + queNo;
        db.execSql(sql);
    }


    /*
     * Утилиты
     */

    private DataRecord loadReplicaRec(long no) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "que_" + queName + " where id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found, no: " + no);
        }
        return rec;
    }

    private void recToReplicaInfo(DataRecord rec, IReplicaInfo info) {
        info.setAge(rec.getValueLong("age"));
        info.setWsId(rec.getValueLong("ws_id"));
        info.setReplicaType(rec.getValueInt("replica_type"));
    }


}
