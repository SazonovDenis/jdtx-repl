package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Общая очередь реплик queCommon на сервере.
 * В отличие от остальных очередей - сама реплики НЕ ХРАНИТ,
 * но при необходимости забирает их из очереди для ws, которую находит в списке srvQueInList
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
     * IJdxReplicaStorage
     */

    @Override
    public void put(IReplica replica, long no) throws Exception {
        // Предполагаем, что файл уже есть в очереди srvQueIn
        //throw new XError("Not supported: JdxQueCommon.put");
    }

    @Override
    public IReplica get(long noQueCommon) throws Exception {
        DataRecord rec = loadReplicaRec(noQueCommon);
        long author_ws_id = rec.getValueLong("author_ws_id");
        long author_id = rec.getValueLong("author_id");

        //
        IJdxQue srvQueIn = srvQueInList.get(author_ws_id);
        IReplica replica = srvQueIn.get(author_id);

        //
        return replica;
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        //
        long replicaNo = replica.getInfo().getNo();
        long replicaAge = replica.getInfo().getAge();
        long replicaWsId = replica.getInfo().getWsId();

        // Проверки: правильность очередности реплик IDE от рабочей станции wsId - обязательно монотонное возрастание возраста replica.age
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE) {
            long queWsMaxAge = getMaxAgeForAuthorWs(replicaWsId);
            if (queWsMaxAge != -1 && replicaAge != queWsMaxAge + 1) {
                throw new XError("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
            }
        }

        // Проверки: правильность номеров реплик от рабочей станции wsId - обязательно монотонное возрастание номера replica.no
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
     * @return Последний возраст (age) реплики в очереди, созданный рабочей станцией wsId
     */
    public long getMaxAgeForAuthorWs(long wsId) throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + queTableName + " where author_ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }


    /**
     * @return Последний номер (author_id) реплики в очереди, созданный рабочей станцией wsId
     */
    private long getMaxNoForAuthorWs(long wsId) throws Exception {
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
