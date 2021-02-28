package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Исходящая очередь на сервере для КАЖДОЙ рабочей станции, сюда распределяем queCommon
 * todo Рефакторинг JdxCommon vs JdxQueOut000 vs JdxQueOut001
 * Важно - JdxQueOut000 особенная - одна таблица на несколько станций, каждая станция - независимо
 */
public class JdxQueOut000 extends JdxQueOut001 {

    //
    public JdxQueOut000(Db db, long wsId) {
        super(db, UtQue.QUE_OUT000, true);
        this.wsId = wsId;
    }

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQueOut000");


    /*
     * IJdxReplicaQue
     */

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        //
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE || replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
            long replicaAge = replica.getInfo().getAge();
            long replicaWsId = replica.getInfo().getWsId();
            long queWsMaxAge = getMaxAgeForWs(replicaWsId);
            if (queWsMaxAge != -1 && replicaAge != queWsMaxAge + 1) {
                //log.error("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
                throw new XError("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge + ", replica.wsId: " + replicaWsId + ", que.name: " + queName);
            }
        }
    }

    /**
     * @return Последний возраст реплики в очереди, созданный рабочей станцией wsId, предназначенный для отправки на destinationWsId
     */
    long getMaxAgeForWs(long wsId) throws Exception {
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


    DataRecord loadReplicaRec(long no) throws Exception {
        String sqlFromWhere = "from " + UtJdx.SYS_TABLE_PREFIX + "que_" + queName + " where destination_ws_id = " + this.wsId;
        //
        String sql = "select * " + sqlFromWhere + " and destination_no = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found, this.wsId" + this.wsId + ", no: " + no);
        }
        return rec;
    }


}
