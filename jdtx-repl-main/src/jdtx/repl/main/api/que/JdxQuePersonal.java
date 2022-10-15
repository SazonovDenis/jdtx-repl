package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Личная очередь реплик (например, queOut).
 */
public class JdxQuePersonal extends JdxQue implements IJdxQue {


    //
    private long authorWsId;

    //
    protected static Log log = LogFactory.getLog("jdtx.QuePersonal");


    //
    public JdxQuePersonal(Db db, String queName, long authorWsId) {
        super(db, queName, UtQue.STATE_AT_WS);
        this.authorWsId = authorWsId;
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        // Проверки: правильность автора - должен быть self.wsId
        long replicaWsId = replica.getInfo().getWsId();
        if (replicaWsId != authorWsId) {
            throw new XError("replica.wsId: " + replicaWsId + ", this.wsId: " + authorWsId);
        }

        // Проверки: правильность очередности реплик по возрасту age
        long queMaxAge = getMaxAge();
        long replicaAge = replica.getInfo().getAge();
        if (replicaAge != -1 && queMaxAge != -1 && replicaAge != queMaxAge + 1) {
            throw new XError("Invalid replica.age: " + replicaAge + ", que.age: " + queMaxAge);
        }
    }


    /*
     * Утилиты
     */


    @Override
    long getReplicaQueNo(IReplica replica) throws Exception {
        // Генерим следующий номер - по порядку
        long queNo = getMaxNo() + 1;

        // Исходно в реплике не должно быть номера или номер в реплике должен совпадать с очередным номером в очереди.
        // Номер не заполнен - при штатном формировании исходящей очереди.
        // Номер уже заполнен - при восстановлении исходящей очереди реплик из папки (во время repairAfterBackupRestore).
        if (replica.getInfo().getNo() != 0 && queNo != replica.getInfo().getNo()) {
            throw new XError("Replica.no already set, replica.no: " + replica.getInfo().getNo() + ", expected que.no: " + queNo);
        }

        // Проставляем номер
        replica.getInfo().setNo(queNo);

        //
        return queNo;
    }


    @Override
    void pushDb(IReplica replica, long queNo) throws Exception {
        String sql = "insert into " + queTableName + " (id, ws_id, age, crc, replica_type) values (:id, :ws_id, :age, :crc, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", queNo,
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "crc", replica.getInfo().getCrc(),
                "replica_type", replica.getInfo().getReplicaType()
        ));
    }


    void recToReplicaInfo(DataRecord rec, IReplicaInfo info) {
        info.setWsId(rec.getValueLong("ws_id"));
        info.setNo(rec.getValueLong("id"));
        info.setAge(rec.getValueLong("age"));
        info.setCrc(rec.getValueString("crc"));
        info.setReplicaType(rec.getValueInt("replica_type"));
    }


    /**
     * @return Последний возраст реплики в очереди для нашей рабочей станции
     */
    public long getMaxAge() throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + queTableName;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            // Признак того, что очередь пуста
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }


}
