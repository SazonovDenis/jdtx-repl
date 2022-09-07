package jdtx.repl.main.api.que;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

import static jdtx.repl.main.api.util.UtJdxErrors.*;

/**
 * Входящая очередь queIn на сервере для КАЖДОЙ рабочей станции.
 * Важно: для queIn одна физическая таблица содержит реплики на несколько станций, каждая станция - независимо.
 * <p>
 * Это зеркало для реплик с филиалов.
 */
public class JdxQueInSrv extends JdxQue {

    //
    protected long authorWsId;

    //
    protected SrvWorkstationStateManager stateManager;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQueInSrv");

    //
    public JdxQueInSrv(Db db, long authorWsId) {
        super(db, UtQue.SRV_QUE_IN, UtQue.STATE_AT_SRV);
        this.authorWsId = authorWsId;
        this.stateManager = new SrvWorkstationStateManager(db);
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public long getMaxNo() throws Exception {
        return stateManager.getValue(authorWsId, "que_" + queName + "_no");
    }

    @Override
    public void setMaxNo(long queNo) throws Exception {
        stateManager.setValue(authorWsId, "que_" + queName + "_no", queNo);
    }


    /*
     * IJdxStorageFile
     */

    @Override
    public void setDataRoot(String dataRoot) {
        String sWsId = UtString.padLeft(String.valueOf(authorWsId), 3, "0");
        String srvQueIn_DirLocal = dataRoot + "srv/que_in_ws_" + sWsId;
        super.setDataRoot(srvQueIn_DirLocal);
    }


    /*
     * Утилиты
     */

    @Override
    void pushDb(IReplica replica, long queNo) throws Exception {
        String sql = "insert into " + queTableName + " (id, author_ws_id, author_id, age, crc, replica_type) values (:id, :author_ws_id, :author_id, :age, :crc, :replica_type)";
        long id = db.loadSql("select gen_id(" + queTableGeneratorName + ", 1) as id from dual").getCurRec().getValueLong("id");
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "author_ws_id", replica.getInfo().getWsId(),
                "author_id", replica.getInfo().getNo(),
                "age", replica.getInfo().getAge(),
                "crc", replica.getInfo().getCrc(),
                "replica_type", replica.getInfo().getReplicaType()
        ));
    }

    @Override
    DataRecord loadReplicaRec(long no) throws Exception {
        String sqlFromWhere = "from " + queTableName + " where author_ws_id = " + this.authorWsId;
        //
        String sql = "select * " + sqlFromWhere + " and author_id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError(message_replicaRecordNotFound + ", queName: " + queName + ", no: " + no + ", this.authorWsId: " + this.authorWsId);
        }
        return rec;
    }


}
