package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class ReplDatabaseStruct_Test extends DbPrepareEtalon_Test {

    // Структуры
    public IJdxDbStruct struct;
    public IJdxDbStruct struct2;
    public IJdxDbStruct struct3;
    public IJdxDbStruct struct5;


    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();


        //
        connectAll(false);


        //
        reloadDbStructAll(false);


        // Чтобы были
        UtFile.mkdirs("temp");
    }

    // Чтение структур всех баз
    public void reloadDbStructAll() throws Exception {
        reloadDbStructAll(true);
    }

    public void reloadDbStructAll(boolean doRaise) throws Exception {
        struct = loadStruct(db, doRaise);
        struct2 = loadStruct(db2, doRaise);
        struct3 = loadStruct(db3, doRaise);
        struct5 = loadStruct(db5, doRaise);
    }

    private IJdxDbStruct loadStruct(Db db, boolean doRaise) throws Exception {
        IJdxDbStruct struct = null;
        try {
            IJdxDbStructReader reader = new JdxDbStructReader();
            reader.setDb(db);
            struct = reader.readDbStruct();
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }
            System.out.println("db.connect: " + e.getMessage());
        }
        return struct;
    }

    @Test
    public void test_db_select() throws Exception {
        // db1
        DataStore st = db.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st);
        // db2
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st2);
        // db3
        DataStore st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st3);
        // db5
        DataStore st5 = db5.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st5);
    }

    public Map<String, Map<String, String>> loadWsDbDataCrc(Db db) throws Exception {
        // Создаем и инициализируем станции ради правильного вызова RefDecodeStrategy.initInstance()
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, ws.getWsId());

        // Ищем разницу
        Map<String, Map<String, String>> dbCrc = UtDbComparer.getDbDataCrc(db, ws.struct, dataSerializer);

        //
        return dbCrc;
    }


}
