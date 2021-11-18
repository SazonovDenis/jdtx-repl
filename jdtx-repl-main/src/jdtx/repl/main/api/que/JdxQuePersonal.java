package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

/**
 * Личная очередь реплик (например, queOut).
 */
public class JdxQuePersonal extends JdxQue implements IJdxQue {


    //
    long wsId;

    //
    protected static Log log = LogFactory.getLog("jdtx.QuePersonal");


    //
    public JdxQuePersonal(Db db, String queName, long wsId) {
        super(db, queName, UtQue.STATE_AT_WS);
        this.wsId = wsId;
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public void validateReplica(IReplica replica) throws Exception {
        super.validateReplica(replica);

        // Проверки: правильность реплик по автору wsId
        long replicaWsId = replica.getInfo().getWsId();
        if (replicaWsId != wsId) {
            throw new XError("replica.wsId: " + replicaWsId + ", this.wsId: " + wsId);
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

    /**
     * @return Последний возраст реплики в очереди для нашей рабочей станции
     */
    public long getMaxAge() throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + UtJdx.SYS_TABLE_PREFIX + "que_" + queName;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
        } else {
            return rec.getValueLong("maxAge");
        }
    }


}
