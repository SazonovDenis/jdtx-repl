package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.junit.*;

public class Update_005_006_state_x_Test extends JdxReplWsSrv_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void testSvc() throws Exception {
        logOn();

        //
        System.out.println(db.getDbSource().getDatabase());

        //
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor script = svc.createByName("Update_005_006_state");
        script.exec(db);
    }

    @Test
    public void testNo() throws Exception {
        logOn();

        //
        prinNo(db);
        prinNo(db2);
        prinNo(db3);
        prinNo(db5);
    }

    public void prinNo(Db db) throws Exception {
        System.out.println(db.getDbSource().getDatabase());

        //
        DbUtils dbu = new DbUtils(db);

        //
        long que_out_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_out").getValueLong("id");
        long que_in_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_in").getValueLong("id");
        long que_common_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_common").getValueLong("id");

        //
        long que_out_no_state = dbu.loadSqlRec("select que_out_no from Z_Z_STATE").getValueLong("que_out_no");
        long que_in_no_state = dbu.loadSqlRec("select que_in_no from Z_Z_STATE").getValueLong("que_in_no");
        long que_common_no_state = dbu.loadSqlRec("select que_common_no from Z_Z_STATE").getValueLong("que_common_no");

        //
        System.out.println("que_out_no_que: " + que_out_no_que + ", que_in_no_que: " + que_in_no_que + ", que_common_no_que " + que_common_no_que);
        System.out.println("que_out_no_state: " + que_out_no_state + ", que_in_no_state: " + que_in_no_state + ", que_common_no_state " + que_common_no_state);
    }

}
