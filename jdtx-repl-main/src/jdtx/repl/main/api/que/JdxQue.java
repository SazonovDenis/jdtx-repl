package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import static jdtx.repl.main.api.util.UtJdxErrors.*;


/**
 * Очередь реплик - хранилище в файлах дополнительно отмечается в БД.
 */
public abstract class JdxQue extends JdxStorageFile implements IJdxQue {

    //
    Db db;

    String queName;
    String queTableName;
    String queTableGeneratorName;

    String stateTableName;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQue");

    //
    public JdxQue(Db db, String queName, boolean stateMode) {
        this.db = db;
        this.queName = UtQue.getQueName(queName);
        this.queTableName = UtQue.getQueTableName(queName);
        this.queTableGeneratorName = UtQue.getQueTableGeneratorName(queName);
        if (stateMode == UtQue.STATE_AT_SRV) {
            this.stateTableName = UtJdx.SYS_TABLE_PREFIX + "SRV_STATE";
        } else {
            this.stateTableName = UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        }
    }

    /*
     * IJdxQueNamed
     */

    public String getQueName() {
        return queName;
    }


    /*
     * IJdxStorageFile
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
        UtJdx.validateReplicaFields(replica);
    }

    public long push(IReplica replica) throws Exception {
        // Установка или проверка номера в очереди
        long queNo = getReplicaQueNo(replica);

        // Проверки: правильность полей реплики
        validateReplica(replica);

        // Вычисляем crc файла данных
        if (replica.getData() != null) {
            String crcFile = UtJdx.getMd5File(replica.getData());
            replica.getInfo().setCrc(crcFile);
        }

        // Помещаем файл данных на место хранения файлов очереди
        if (replica.getData() != null) {
            put(replica, queNo);
        }

        // Отмечаем в БД
        db.startTran();
        try {
            pushDb(replica, queNo);

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
        String sql = "select * from " + stateTableName;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + stateTableName + " set que_" + queName + "_no = " + queNo;
        db.execSql(sql);
    }


    /*
     * Утилиты
     */

    long getReplicaQueNo(IReplica replica) throws Exception {
        // Генерим следующий номер в НАШЕЙ очереди - по порядку.
        long queNo = getMaxNo() + 1;

        // В самой реплике номер либо УЖЕ ДОЛЖЕН БЫТЬ (тот номер, который пришел от рабочей станции),
        // либо его тут не нужно (это реплика системная от сервера),
        // поэтому тут значение replica.info.no - не присваиваем (проверку делает validateReplicaFields)

        //
        return queNo;
    }

    void pushDb(IReplica replica, long queNo) throws Exception {
        String sql = "insert into " + queTableName + " (id, author_ws_id, author_id, age, crc, replica_type) values (:id, :author_ws_id, :author_id, :age, :crc, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", queNo,
                "author_ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "author_id", replica.getInfo().getNo(),
                "crc", replica.getInfo().getCrc(),
                "replica_type", replica.getInfo().getReplicaType()
        ));
    }

    void recToReplicaInfo(DataRecord rec, IReplicaInfo info) {
        info.setWsId(rec.getValueLong("author_ws_id"));
        info.setAge(rec.getValueLong("age"));
        info.setNo(rec.getValueLong("author_id"));
        info.setCrc(rec.getValueString("crc"));
        info.setReplicaType(rec.getValueInt("replica_type"));
    }

    DataRecord loadReplicaRec(long no) throws Exception {
        String sql = "select * from " + queTableName + " where id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError(message_replicaNotFound + ", queName: " + queName + ", no: " + no);
        }
        return rec;
    }


}
