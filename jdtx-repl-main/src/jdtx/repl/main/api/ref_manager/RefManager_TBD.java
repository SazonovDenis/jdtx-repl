package jdtx.repl.main.api.ref_manager;

import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;

/**
 * Реализация IRefManager, если разведение pk основано на диапазонах как в ТБД
 */
public class RefManager_TBD extends RefManagerService implements IRefManager {


    // ------------------------------------------
    // RefManagerService
    // ------------------------------------------

    @Override
    public IJdxDataSerializer createDataSerializer() {
        return new JdxDataSerializerPlain();
    }


    // ------------------------------------------
    // IRefManager
    // ------------------------------------------

    @Override
    public long get_id_local(String tableName, JdxRef ref) throws Exception {
        return ref.value;
    }

    @Override
    public JdxRef get_ref(String tableName, long id_local) throws Exception {
        JdxRef ref = new JdxRef();

        ref.value = id_local;

        return ref;
    }

    @Override
    public long get_max_own_id(IJdxTable table) throws Exception {
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();

        //
        String sql = "select\n" +
                " max(" + pkFieldName + ") as maxId,\n" +
                " mod(" + pkFieldName + ", 10) as modId,\n" +
                " max(case when " + pkFieldName + " >= :idFrom then mod(" + pkFieldName + ", 10) else 0 end) as modIdFrom\n" +
                "from\n" +
                " " + tableName + "\n" +
                "where\n" +
                " mod(" + pkFieldName + ", 10) = :wsId\n" +
                "group by\n" +
                " mod(" + pkFieldName + ", 10)";
        long wsId = ws.getWsId();
        long maxId = db.loadSql(sql, UtCnv.toMap("idFrom", 10000000000L, "wsId", wsId)).getCurRec().getValueLong("maxId");

        //
        return maxId;
    }

    @Override
    public boolean isPresent_not_own_id(IJdxTable table) throws Exception {
        return false;
    }


}
