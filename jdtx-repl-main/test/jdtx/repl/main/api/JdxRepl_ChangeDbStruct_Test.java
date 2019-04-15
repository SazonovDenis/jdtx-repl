package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class JdxRepl_ChangeDbStruct_Test extends JdxReplWsSrv_Test {


    @Test
    public void test_changeDbStruct() throws Exception {
        // Меняем данные на рабочих станциях
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Формируем сигнал "всем молчать"
        test_srvMuteAll();

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Ждем ответа на сигнал

        // Меняем данные на рабочих станциях
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Убеждаемся что рабочие станции молчат (из-из запрета)
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        // Меняем свою структуру
        test_ws_changeDbStruct(db);

        // Рассылаем сигнал "всем говорить"
        test_srvUnmuteAll();

        // Меняем данные на рабочих станциях
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Убеждаемся что рабочие станции молчат (из-за несовпадения струтуры)
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        // Меняем структуру на рабочих станциях
        test_ws_changeDbStruct(db2);
        test_ws_changeDbStruct(db3);

        // Убеждаемся что рабочие станции говорят
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        // Цикл синхронизации
        sync_http();
        sync_http();

        //
        test_dumpTables();
    }

    @Test
    public void test_ws1_changeDbStruct() throws Exception {
        test_ws_changeDbStruct(db);
    }

    @Test
    public void test_ws1_changeDb2Struct() throws Exception {
        test_ws_changeDbStruct(db2);
    }

    @Test
    public void test_ws1_changeDb3Struct() throws Exception {
        test_ws_changeDbStruct(db3);
    }

    void test_ws_changeDbStruct(Db db) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct;

        //
        struct = reader.readDbStruct();
        struct_rw.write(struct, "../_test-data/dbStruct_0.xml");

        //
        UtTest utTest = new UtTest(db);
        utTest.changeDbStruct("region");

        //
        struct = reader.readDbStruct();
        struct_rw.write(struct, "../_test-data/dbStruct_1.xml");
    }

    @Test
    /**
     * Проверяет корректность формирования аудита при цикле вставки и удаления влияющей записи:
     */
    public void test_auditAfterInsDel() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.make_InsDel(struct2, 2);

        // Формирование аудита
        test_ws2_handleSelfAudit();
    }


}
