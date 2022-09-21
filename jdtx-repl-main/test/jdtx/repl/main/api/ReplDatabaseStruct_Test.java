package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class ReplDatabaseStruct_Test extends DbPrepareEtalon_Test {

    // Структуры
    public IJdxDbStruct struct_one;
    public IJdxDbStruct struct;
    public IJdxDbStruct struct2;
    public IJdxDbStruct struct3;
    public IJdxDbStruct struct5;


    @Override
    public void setUp() throws Exception {
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

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        // Отключение от БД при завершении теста - важно при пакетном запуске тестов
        disconnectAllForce();
    }

    // Чтение структур всех баз
    public void reloadDbStructAll() throws Exception {
        reloadDbStructAll(true);
    }

    public void reloadDbStructAll(boolean doRaise) throws Exception {
        struct_one = loadStruct(db_one, doRaise);
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
            System.out.println("db: " + (new File(db.getDbSource().getDatabase()).getCanonicalPath()));
            if (doRaise) {
                throw e;
            } else {
                System.out.println("db.connect: " + e.getMessage());
            }
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
        // Создаем и инициализируем станции ради
        //  - правильного вызова RefDecodeStrategy.initInstance()
        //  - ws.struct и ws.wsId
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        IJdxDataSerializer dataSerializer = db.getApp().service(DataSerializerService.class);

        // Собираем отпечаток crc записей таблиц
        Map<String, Map<String, String>> dbCrc = UtDbComparer.getDbDataCrc(db, ws.struct, dataSerializer);

        //
        return dbCrc;
    }

    public void compareDb(Db db, Db db2, Map<String, String> compareResultExpected) throws Exception {
        boolean bad = false;

        System.out.println("compare: " + db.getDbSource().getDatabase());
        System.out.println("     vs: " + db2.getDbSource().getDatabase());

        //
        //System.out.println("db A");
        Map<String, Map<String, String>> dbCrcSrv = loadWsDbDataCrc(db);
        //System.out.println();

        //System.out.println("db B");
        Map<String, Map<String, String>> dbCrcWs2 = loadWsDbDataCrc(db2);
        //System.out.println();

        //
        Map<String, Set<String>> diffCrc = new HashMap<>();
        Map<String, Set<String>> diffNewIn1 = new HashMap<>();
        Map<String, Set<String>> diffNewIn2 = new HashMap<>();
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs2, diffCrc, diffNewIn1, diffNewIn2);

        // Сравним разницу между базами с ожиданием
        for (String tableName : compareResultExpected.keySet()) {
            Set<String> result_diffCrc = diffCrc.get(tableName);
            Set<String> result_diffNewIn1 = diffNewIn1.get(tableName);
            Set<String> result_diffNewIn2 = diffNewIn2.get(tableName);

            String tableExpectedResult = compareResultExpected.get(tableName);
            char expectedCrc = tableExpectedResult.charAt(0);
            char expectedNewIn1 = tableExpectedResult.charAt(1);
            char expectedNewIn2 = tableExpectedResult.charAt(2);

            switch (expectedCrc) {
                case 'N': {
                    if (result_diffCrc.size() != 0) {
                        bad = true;
                        System.out.println(tableName + ", found crc");
                        for (String recStr : result_diffCrc) {
                            System.out.println("  " + recStr);
                        }
                    }
                    break;
                }
                case 'Y': {
                    if (result_diffCrc.size() == 0) {
                        bad = true;
                        System.out.println(tableName + ": not found crc");
                    }
                    break;
                }
                case '?': {
                    break;
                }
                default:
                    throw new XError("Bad expected symbol: " + expectedCrc);
            }

            switch (expectedNewIn1) {
                case 'N': {
                    if (result_diffNewIn1.size() != 0) {
                        bad = true;
                        System.out.println(tableName + ", found new in 1");
                        for (String recStr : result_diffNewIn1) {
                            System.out.println("  " + recStr);
                        }
                    }
                    break;
                }
                case 'Y': {
                    if (result_diffNewIn1.size() == 0) {
                        bad = true;
                        System.out.println(tableName + ": not found new in 1");
                    }
                    break;
                }
                case '?': {
                    break;
                }
                default:
                    throw new XError("Bad expected symbol: " + expectedNewIn1);
            }

            switch (expectedNewIn2) {
                case 'N': {
                    if (result_diffNewIn2.size() != 0) {
                        bad = true;
                        System.out.println(tableName + ", found new in 2");
                        for (String recStr : result_diffNewIn2) {
                            System.out.println("  " + recStr);
                        }
                    }
                    break;
                }
                case 'Y': {
                    if (result_diffNewIn2.size() == 0) {
                        bad = true;
                        System.out.println(tableName + ": not found new in 2");
                    }
                    break;
                }
                case '?': {
                    break;
                }
                default:
                    throw new XError("Bad expected symbol: " + expectedNewIn2);
            }
        }

        //
        if (bad) {
            assertEquals("Обнаружена разница", false, bad);
        }
    }

}
