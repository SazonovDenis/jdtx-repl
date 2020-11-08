package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Общая очередь реплик (например, queCommon).
 * Физическая реализация очереди хранения реплик.
 */
public class JdxQueCommon extends JdxQue implements IJdxQue {

    //
    protected static Log log = LogFactory.getLog("jdtx.QueCommon");


    //
    public JdxQueCommon(Db db, String queName, boolean queForEachWsAtSrv) {
        super(db, queName, queForEachWsAtSrv);
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        // Проверки: правильность очередности реплик для рабочей станции wsId - монотонное возрастание возраста replica.age
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE || replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
            long replicaAge = replica.getInfo().getAge();
            long replicaWsId = replica.getInfo().getWsId();
            long queWsMaxAge = getMaxAgeForWs(replicaWsId);
            if (queWsMaxAge != -1 && replicaAge != queWsMaxAge + 1) {
                throw new XError("invalid replica.age: " + replicaAge + ", que.age: " + queWsMaxAge);
            }
        }
    }


    /*
     * Утилиты
     */

    /**
     * @return Последний возраст реплики в очереди для рабочей станции wsId
     */
    private long getMaxAgeForWs(long wsId) throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que_" + queName + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxAge");
        }
    }


}
