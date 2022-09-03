package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Общая очередь реплик queCommon на сервере.
 * Сама реплики может НЕ ХРАНИТЬ, но при необходимости забирает их из "зеркальной" входящей очереди для ws,
 * которую находит в списке srvQueInList
 */
public class JdxQueCommon extends JdxQue implements IJdxQueCommon {


    Map<Long, IJdxQue> srvQueInList;

    //
    protected static Log log = LogFactory.getLog("jdtx.QueCommon");

    //
    public JdxQueCommon(Db db, String queName, boolean stateMode) {
        super(db, queName, stateMode);
    }

    /*
     * IJdxStorageFile
     */

    @Override
    public void setDataRoot(String dataRoot) {
        super.setDataRoot(dataRoot + "srv/que_common");
    }

    /*
     * IJdxReplicaStorage
     */

    @Override
    public void put(IReplica replica, long no) throws Exception {
        if (isServerReplica(replica.getInfo())) {
            // Реплика, сформированная самим сервером?
            // Для таких реплик сохраняем ФАЙЛ реплики в очереди QueCommon обычным образом.
            super.put(replica, no);
        } else {
            // Предполагаем, что ФАЙЛ реплики остается в очереди srvQueIn,
            // поэтому никаких действий по размещению ФАЙЛА реплики в очереди - не требуется,
            // потребуется только помещение в саму очередь, но это делается командой push.
            log.trace("");
        }
    }

    @Override
    public IReplica get(long noQueCommon) throws Exception {
        IReplica replica;

        //
        DataRecord rec = loadReplicaRec(noQueCommon);
        IReplicaInfo replicaInfo = new ReplicaInfo();
        recToReplicaInfo(rec, replicaInfo);
        //
        if (isServerReplica(replicaInfo)) {
            // Реплика, сформированная самим сервером?
            // Для таких реплик берем ФАЙЛ реплики из очереди QueCommon обычным образом.
            replica = super.get(noQueCommon);
        } else {
            // Берем реплику из очереди SrvQueIn, узнаем только, какой рабочй станции она
            IJdxQue srvQueIn = srvQueInList.get(replicaInfo.getWsId());
            // Реплику берем по её номеру.
            replica = srvQueIn.get(replicaInfo.getNo());
        }

        //
        return replica;
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    void pushDb(IReplica replica, long queNo) throws Exception {
        String sql = "insert into " + queTableName + " (id, author_ws_id, author_id, age, crc, replica_type) values (:id, :author_ws_id, :author_id, :age, :crc, :replica_type)";
        Map values = UtCnv.toMap(
                "id", queNo,
                "author_ws_id", replica.getInfo().getWsId(),
                "author_id", replica.getInfo().getNo(),
                "age", replica.getInfo().getAge(),
                "crc", replica.getInfo().getCrc(),
                "replica_type", replica.getInfo().getReplicaType()
        );
        if (isServerReplica(replica.getInfo())) {
            // Из-за требований уникального индекса
            // ZZ_Z_srv_que_common_idx в таблице ZZ_Z_srv_que_common (author_ws_id + author_id)
            values.put("author_id", -queNo);
        }
        db.execSql(sql, values);
    }

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        // Сереверная реплика - не проверяем номера
        if (isServerReplica(replica.getInfo())) {
            return;
        }

        //
        long replicaNo = replica.getInfo().getNo();
        long replicaAge = replica.getInfo().getAge();
        long replicaWsId = replica.getInfo().getWsId();

        // Правильность возраста (age) для реплик IDE от рабочей станции wsId.
        // Очередь либо пуста (queWsMaxNo == -1), либо обязательно монотонное возрастание возраста replica.age
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE) {
            long queWsMaxAge = getMaxAgeForAuthorWs(replicaWsId);
            // Очередь либо пуста (queWsMaxNo == -1), либо пронумерована правильно
            if (queWsMaxAge != -1 && replicaAge != queWsMaxAge + 1) {
                throw new XError("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
            }
        }

        // Правильность номеров (no) реплик от рабочей станции wsId.
        // Очередь либо пуста (queWsMaxNo == -1), либо обязательно монотонное возрастание номера replica.no
        long queWsMaxNo = getMaxNoForAuthorWs(replicaWsId);
        if (queWsMaxNo != -1 && replicaNo != queWsMaxNo + 1) {
            throw new XError("invalid replica.no: " + replicaNo + ", que.no: " + queWsMaxNo + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
        }
    }


    /*
     * IJdxQueCommon
     */

    @Override
    public void setSrvQueIn(Map<Long, IJdxQue> srvQueInList) {
        this.srvQueInList = srvQueInList;
    }


    @Override
    public long getNoByAuthorNo(long authorNo, long wsId) throws Exception {
        String sql = "select id from " + queTableName + " where author_ws_id = " + wsId + " and author_id = " + authorNo;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") <= 0) {
            throw new XError("Replica number not found in queCommon, authorWs: " + wsId + ", authorNo: " + authorNo);
        } else {
            return rec.getValueLong("id");
        }
    }


    /*
     * Утилиты
     */

    /**
     * Бывают реплики, формируемые самим сервером (а не серверной рабочей станцией),
     * например, системные реплики, а также snapshot на станции присмене версии БД
     * попадут в QueCommon не через очередь станций, а непосредственно с сервера.
     *
     * @return сформированна ли реплика самим сервером
     */
    private boolean isServerReplica(IReplicaInfo replicaInfo) {
        return replicaInfo.getNo() <= 0 && (replicaInfo.getWsId() <= 0 || replicaInfo.getWsId() == JdxReplSrv.SERVER_WS_ID);
    }

    /**
     * @return Последний возраст (age) реплики в очереди, созданный рабочей станцией wsId
     */
    public long getMaxAgeForAuthorWs(long wsId) throws Exception {
        if (wsId == 0) {
            // Это бывает для системных реплик, отправленных от самого сервера
            return -1;
        } else {
            String sql = "select max(age) as maxAge, count(*) as cnt from " + queTableName + " where author_ws_id = " + wsId;
            DataRecord rec = db.loadSql(sql).getCurRec();
            if (rec.getValueLong("cnt") == 0) {
                return -1;
            } else {
                return rec.getValueLong("maxAge");
            }
        }
    }


    /**
     * @return Последний номер (author_id) реплики в очереди, созданный рабочей станцией wsId
     */
    private long getMaxNoForAuthorWs(long wsId) throws Exception {
        if (wsId == 0) {
            // Это бывает для системных реплик, отправленных от самого сервера
            return -1;
        } else {
            String sql = "select max(author_id) as maxNo, count(*) as cnt from " + queTableName + " where author_ws_id = " + wsId;
            DataRecord rec = db.loadSql(sql).getCurRec();
            if (rec.getValueLong("cnt") == 0) {
                // Очередь queCommon для этой wsId пуста
                return -1;
            } else if (rec.getValueLong("maxNo") < 0) {
                // Очередь queCommon для этой wsId содержит реплики старого формата, когда реплики не содержали author_id
                return -1;
            } else {
                return rec.getValueLong("maxNo");
            }
        }
    }


}
