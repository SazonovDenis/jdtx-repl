package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

import java.util.*;

import static jdtx.repl.main.api.util.UtJdxErrors.*;


/**
 * Исходящая очередь queOut000 и queOut001 на сервере для КАЖДОЙ рабочей станции.
 * Важно: для QueOut000 и QueOut001 одна физическая таблица содержит реплики на несколько станций, каждая станция - независимо.
 * <p>
 * В очередь queOut000 распределяем queCommon.
 */
public abstract class JdxQueOutSrv extends JdxQue implements IJdxQue {

    //
    protected long destinationWsId;

    //
    protected SrvWorkstationStateManager stateManager;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQueOutSrv");

    //
    public JdxQueOutSrv(Db db, String queName, boolean stateMode) {
        super(db, queName, stateMode);
        stateManager = new SrvWorkstationStateManager(db);
    }


    /*
     * IJdxStorageFile
     */

    @Override
    public void setDataRoot(String dataRoot) {
        String sWsId = UtString.padLeft(String.valueOf(destinationWsId), 3, "0");
        String que001_DirLocal = dataRoot + "srv/que_" + queName + "_ws_" + sWsId;
        super.setDataRoot(que001_DirLocal);
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public long getMaxNo() throws Exception {
        return stateManager.getValue(destinationWsId, "que_" + queName + "_no");
    }

    @Override
    public void setMaxNo(long queNo) throws Exception {
        stateManager.setValue(destinationWsId, "que_" + queName + "_no", queNo);
    }


    /*
     * Утилиты
     */

    long getReplicaQueNo(IReplica replica) throws Exception {
        // Генерим следующий номер в нашей очереди - по порядку.
        long queNo = getMaxNo() + 1;

        // В самой реплике номер либо УЖЕ ДОЛЖЕН БЫТЬ (тот номер, который пришел от рабочей станции),
        // либо его тут не нужно (это реплика системная от сервера),
        // поэтому тут значение replica.info.no - не присваиваем (проверку делает validateReplicaFields)

        // Проверки: правильность номеров реплик от рабочей станции wsId - обязательно монотонное возрастание номера replica.no
        // Эта проверка избыточна - есть уникальный индекс destination_ws_id+destination_id,
        // а монотонное возрастание гарантируется нашей собственной getMaxNo()

        //
        return queNo;
    }

    @Override
    void pushDb(IReplica replica, long queNo) throws Exception {
        long id = db.loadSql("select gen_id(" + queTableGeneratorName + ", 1) as id from dual").getCurRec().getValueLong("id");
        Map params = UtCnv.toMap(
                "id", id,
                "destination_ws_id", destinationWsId,
                "destination_id", queNo,
                "author_ws_id", replica.getInfo().getWsId(),
                "author_id", replica.getInfo().getNo(),
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "crc", replica.getInfo().getCrc(),
                "replica_type", replica.getInfo().getReplicaType()
        );

        //
        String sql = "insert into " + queTableName + " (id, destination_ws_id, destination_id, author_ws_id, author_id, age, crc, replica_type) values (:id, :destination_ws_id, :destination_id, :author_ws_id, :author_id, :age, :crc, :replica_type)";
        db.execSql(sql, params);
    }

    @Override
    DataRecord loadReplicaRec(long no) throws Exception {
        String sqlFromWhere = "from " + queTableName + " where destination_ws_id = " + this.destinationWsId;
        //
        String sql = "select * " + sqlFromWhere + " and destination_id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError(message_replicaRecordNotFound + ", queName: " + queName + ", no: " + no + ", this.destinationWsId: " + this.destinationWsId);
        }
        return rec;
    }


}
