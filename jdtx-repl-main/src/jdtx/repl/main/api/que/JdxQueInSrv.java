package jdtx.repl.main.api.que;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

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
    protected static Log log = LogFactory.getLog("jdtx.JdxQueInSrv");

    //
    public JdxQueInSrv(Db db, long authorWsId) {
        super(db, UtQue.SRV_QUE_IN, UtQue.STATE_AT_SRV);
        this.authorWsId = authorWsId;
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public long getMaxNo() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " where ws_id = " + authorWsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    @Override
    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE" + " set que_" + queName + "_no = " + queNo + " where ws_id = " + authorWsId;
        db.execSql(sql);
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
        JdxDbUtils dbu = new JdxDbUtils(db, null);
        long id = dbu.getNextGenerator(queTableGeneratorName);
        //
        String sql = "insert into " + queTableName + " (id, author_ws_id, author_id, age, crc, replica_type) values (:id, :author_ws_id, :author_id, :age, :crc, :replica_type)";
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
            throw new XError("Replica not found, this.authorWsId: " + this.authorWsId + ", queName: " + queName + ", no: " + no);
        }
        return rec;
    }


}
