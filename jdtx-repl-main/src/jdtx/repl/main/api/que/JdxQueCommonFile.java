package jdtx.repl.main.api.que;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.DbUtils;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

/**
 * Формирователь общей очереди реплик.
 * Физическая реализация хранения реплик и их упорядочивания.
 */
public class JdxQueCommonFile extends JdxQueFile implements IJdxQueCommon {

    //
    private Db db;

    //
    protected static Log log = LogFactory.getLog("jdtx.QueCommon");

    //
    public JdxQueCommonFile(Db db, String queName) {
        this.db = db;
        this.queName = JdxQueName.getTableSuffix(queName);
    }

    @Override
    public IReplica get(long no) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "que_" + queName + " where id = " + no;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Replica not found, no: " + no);
        }

        //
        IReplica replica = super.get(no);

        //
        replica.getInfo().setAge(rec.getValueLong("age"));
        replica.getInfo().setWsId(rec.getValueLong("ws_id"));
        replica.getInfo().setReplicaType(rec.getValueInt("replica_type"));

        //
        return replica;
    }

    @Override
    public long push(IReplica replica) throws Exception {
        validateReplica(replica);

        // Генерим следующий номер
        long queNo = getMaxNo() + 1;

        // Помещаем файл на место хранения файлов очереди.
        if (replica.getFile() != null) {
            put(replica, queNo);
        }

        // Отмечаем в БД
        DbUtils dbu = new DbUtils(db, null);
        long id = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "que_" + queName);
        String sql = "insert into " + JdxUtils.sys_table_prefix + "que_" + queName + " (id, ws_id, age, replica_type) values (:id, :ws_id, :age, :replica_type)";
        db.execSql(sql, UtCnv.toMap(
                "id", id,
                "ws_id", replica.getInfo().getWsId(),
                "age", replica.getInfo().getAge(),
                "replica_type", replica.getInfo().getReplicaType()
        ));

        //
        return queNo;
    }

    /**
     * @deprecated
     */
    public long getMaxAge(long wsId) throws Exception {
        String sql = "select max(age) as maxAge, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que_" + queName + " where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            return 0;
        } else {
            return rec.getValueLong("maxAge");
        }
    }

    @Override
    public long getMaxNo() throws Exception {
        String sql = "select max(id) as maxNo, count(*) as cnt from " + JdxUtils.sys_table_prefix + "que_" + queName;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("cnt") == 0) {
            // Очередь пуста - вернем 0.
            // Номера в очередях начинаются от 1 (в отличие от возрастов, котрые могут начаться с 0)
            return 0;
        } else {
            return rec.getValueLong("maxNo");
        }
    }

    void validateReplica(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        JdxUtils.validateReplicaFields(replica);

        // Проверки: правильность очередности реплик по возрасту age для рабочей станции wsId
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE || replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
            long queMaxAge = getMaxAge(replica.getInfo().getWsId());
            if (replica.getInfo().getAge() != queMaxAge + 1) {
                throw new XError("invalid replica.age: " + replica.getInfo().getAge() + ", que.age: " + queMaxAge);
            }
        }

    }

}
