package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;


/**
 * Исходящая очередь queOut001 на сервере для КАЖДОЙ рабочей станции,
 * В эти очереди распределяем queCommon.
 * <p>
 * Важно - JdxQueOut001 особенная - одна физическая таблица содержит реплики на несколько станций, каждая станция - независимо
 * <p>
 * todo Рефакторинг JdxCommon vs JdxQueOut000 vs JdxQueOut001 vs JdxQue
 */
public class JdxQueOut001 extends JdxQue implements IJdxQue {

    //
    long wsId;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQueOut001");

    //
    public JdxQueOut001(Db db, long wsId) {
        super(db, UtQue.QUE_OUT001, UtQue.STATE_AT_SRV);
        this.wsId = wsId;
    }

    public JdxQueOut001(Db db, String queName, boolean stateMode) {
        super(db, queName, stateMode);
    }


    /*
     * IJdxStorageFile
     */

    @Override
    public void setDataRoot(String dataRoot) {
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String que001_DirLocal = dataRoot + "srv/que_" + queName + "_ws_" + sWsId;
        super.setDataRoot(que001_DirLocal);
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public long push(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        validateReplica(replica);

        // Генерим следующий номер - по порядку
        long queNo = getMaxNo() + 1;

        // Вычисляем crc файла данных
        if (replica.getData() != null) {
            String crc = UtJdx.getMd5File(replica.getData());
            replica.getInfo().setCrc(crc);
        }

        // Помещаем файл на место хранения файлов очереди.
        if (replica.getData() != null) {
            put(replica, queNo);
        }

        // Отмечаем в БД
        db.startTran();
        try {
            JdxDbUtils dbu = new JdxDbUtils(db, null);
            long id = dbu.getNextGenerator(UtJdx.SYS_GEN_PREFIX + "que_" + queName);
            //
            String sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "que_" + queName + " (id, destination_ws_id, destination_no, ws_id, age, crc, replica_type) values (:id, :destination_ws_id, :destination_no, :ws_id, :age, :crc, :replica_type)";
            db.execSql(sql, UtCnv.toMap(
                    "id", id,
                    "destination_ws_id", wsId,
                    "destination_no", queNo,
                    "ws_id", replica.getInfo().getWsId(),
                    "age", replica.getInfo().getAge(),
                    "crc", replica.getInfo().getCrc(),
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
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " set que_" + queName + "_no = " + queNo + " where ws_id = " + wsId;
        db.execSql(sql);
    }

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        // Проверки: правильность очередности реплик IDE для рабочей станции wsId - обязательно монотонное возрастание возраста replica.age
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE) {
            long replicaAge = replica.getInfo().getAge();
            long replicaWsId = replica.getInfo().getWsId();
            long queWsMaxAge = getMaxAgeForWs(replicaWsId);
            if (queWsMaxAge != -1 && replicaAge != queWsMaxAge + 1) {
                throw new XError("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
            }
        }
    }

    /**
     * @return Последний возраст реплики в очереди, созданный рабочей станцией wsId, предназначенный для отправки на destinationWsId
     */
    @Override
    public long getMaxAgeForWs(long wsId) throws Exception {
        String sqlFromWhere = "from " + UtJdx.SYS_TABLE_PREFIX + "que_" + queName + " where destination_ws_id = " + this.wsId;
        //
        String sql = "select max(age) as maxAge, count(*) as cnt " + sqlFromWhere + " and ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }


    @Override
    DataRecord loadReplicaRec(long no) throws Exception {
        String sqlFromWhere = "from " + UtJdx.SYS_TABLE_PREFIX + "que_" + queName + " where destination_ws_id = " + this.wsId;
        //
        String sql = "select * " + sqlFromWhere + " and destination_no = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found, this.wsId: " + this.wsId + ", queName: " + queName + ", no: " + no);
        }
        return rec;
    }


}
