package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;


// todo Рефакторинг JdxCommon vs JdxQueOut000 vs JdxQueOut001
public class JdxQueOut001 extends JdxQue implements IJdxQue {

    //
    long wsId;

    //
    public JdxQueOut001(Db db, long wsId) {
        super(db, UtQue.QUE_OUT001, true);
        this.wsId = wsId;
    }

    public JdxQueOut001(Db db, String queName, boolean queForEachWsAtSrv) {
        super(db, queName, queForEachWsAtSrv);
    }


    /*
     * IJdxStorageFile
     */

    @Override
    public void setDataRoot(String dataRoot) {
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String que001_DirLocal = dataRoot + "srv/que_" + queName + "_ws_" + sWsId;
        super.setDataRoot(que001_DirLocal);
    }


    /*
     * IJdxReplicaQue
     */

    @Override
    public long push(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        validateReplica(replica);

        // Генерим следующий номер - по порядку
        long queNo = getMaxNo() + 1;

        // Помещаем файл на место хранения файлов очереди.
        if (replica.getFile() != null) {
            put(replica, queNo);
        }

        // Отмечаем в БД
        db.startTran();
        try {
            JdxDbUtils dbu = new JdxDbUtils(db, null);
            long id = dbu.getNextGenerator(JdxUtils.SYS_GEN_PREFIX + "que_" + queName);
            //
            String sql = "insert into " + JdxUtils.SYS_TABLE_PREFIX + "que_" + queName + " (id, destination_ws_id, destination_no, ws_id, age, replica_type) values (:id, :destination_ws_id, :destination_no, :ws_id, :age, :replica_type)";
            db.execSql(sql, UtCnv.toMap(
                    "id", id,
                    "destination_ws_id", wsId,
                    "destination_no", queNo,
                    "ws_id", replica.getInfo().getWsId(),
                    "age", replica.getInfo().getAge(),
                    "replica_type", replica.getInfo().getReplicaType()
            ));

            //
            setMaxNo(queNo);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        //
        return queNo;
    }

    public long getMaxNo() throws Exception {
        String sql = "select * from " + JdxUtils.SYS_TABLE_PREFIX + "state_ws" + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no");
    }

    public void setMaxNo(long queNo) throws Exception {
        String sql = "update " + JdxUtils.SYS_TABLE_PREFIX + "state_ws" + " set que_" + queName + "_no = " + queNo + " where ws_id = " + wsId;
        db.execSql(sql);
    }


}
