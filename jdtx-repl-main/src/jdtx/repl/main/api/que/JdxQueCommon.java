package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Общая очередь реплик (например, queCommon).
 * Физическая реализация очереди хранения реплик.
 * todo Рефакторинг JdxCommon vs JdxQueOut000 vs JdxQueOut001
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


}
