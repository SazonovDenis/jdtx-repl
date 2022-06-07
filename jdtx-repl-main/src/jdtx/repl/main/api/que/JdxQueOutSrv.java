package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
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
    protected static Log log = LogFactory.getLog("jdtx.JdxQueOutSrv");

    //
    public JdxQueOutSrv(Db db, String queName, boolean stateMode) {
        super(db, queName, stateMode);
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
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " where ws_id = " + destinationWsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    @Override
    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " set que_" + queName + "_no = " + queNo + " where ws_id = " + destinationWsId;
        db.execSql(sql);
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
        long queWsMaxNo = getMaxNoForDestinationWs(destinationWsId);
        if (queWsMaxNo != -1 && queNo != queWsMaxNo + 1) {
            throw new XError("invalid replica.no: " + queNo + ", que.no: " + queWsMaxNo + ", destinationWsId: " + destinationWsId + ", que.name: " + queName);
        }

        //
        return queNo;
    }

    @Override
    void pushDb(IReplica replica, long queNo) throws Exception {
        JdxDbUtils dbu = new JdxDbUtils(db, null);
        long id = dbu.getNextGenerator(queTableGeneratorName);
        //
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

    /**
     * @return Последний номер (destination_id) в очереди, предназначенный для отправки на destinationWsId
     */
    private long getMaxNoForDestinationWs(long wsId) throws Exception {
        String sql = "select max(destination_id) as maxNo, count(*) as cnt from " + queTableName + " where destination_ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return -1;
        } else {
            return rec.getValueLong("maxNo");
        }
    }


}
