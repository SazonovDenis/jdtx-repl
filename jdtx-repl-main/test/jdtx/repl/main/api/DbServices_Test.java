package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

/**
 *
 */
public class DbServices_Test extends AppTestCase {


    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();
    }

    /**
     * Коннект к БД
     */
    @Test
    public void test_db() throws Exception {
        Model model = app.getApp().service(ModelService.class).getModel("default");
        Db db = model.getDb();

        System.out.println("db: " + UtJdx.getDbInfoStr(db));
        System.out.println("connect...");
        db.connect();
        System.out.println("connect ok");

        System.out.println();
        System.out.println("select...");
        DataStore st = db.loadSql("select DUAL.*, 'qwerty' as test from DUAL");
        UtData.outTable(st);

        System.out.println();
        System.out.println("disconnect...");
        db.disconnect();
        System.out.println("disconnect ok");
    }

    @Test
    public void test_DbErrorsService() throws Exception {
        Model model = app.getApp().service(ModelService.class).getModel("default");
        Db db = model.getDb();

        IDbErrors dbErrors = db.service(DbErrorsService.class);

        System.out.println("db: " + UtJdx.getDbInfoStr(db));
        System.out.println("dbErrors: " + dbErrors.getClass());
    }

    @Test
    public void testSvc() throws Exception {
        IAppPkRules appPkRules = app.service(AppPkRulesService.class);

        System.out.println("appPkRules: " + appPkRules.getClass());

        String tableName = "Lic";
        System.out.println(tableName + ".generator.name: " + appPkRules.getGeneratorName(tableName));
    }

    @Test
    public void test_DbObjectManager() throws Exception {
        Model model = app.getApp().service(ModelService.class).getModel("default");
        Db db = model.getDb();

        IDbObjectManager dbObjectManager = db.service(DbObjectManager.class);
        IDbDatatypeManager dbDatatypeManager = db.service(DbDatatypeManager.class);

        System.out.println("db: " + UtJdx.getDbInfoStr(db));
        System.out.println("dbObjectManager: " + dbObjectManager.getClass());
        System.out.println("dbDatatypeManager: " + dbDatatypeManager.getClass());
    }

    @Test
    public void test_DbGeneratorsService() throws Exception {
        String generatorName = "TEST";
        long generatorStartValue = 10;
        long generatorSetLastValue = 123;

        Model model = app.getApp().service(ModelService.class).getModel("default");
        Db db = model.getDb();

        IDbGenerators dbGenerators = db.service(DbGeneratorsService.class);

        System.out.println("db: " + UtJdx.getDbInfoStr(db));
        System.out.println("dbGenerators: " + dbGenerators.getClass());

        db.connect();

        try {
            dbGenerators.dropGenerator(generatorName);
        } catch (Exception e) {
        }

        System.out.println();
        System.out.println("createGenerator: " + generatorName);
        dbGenerators.createGenerator(generatorName);

        dbGenerators.setLastValue(generatorName, generatorStartValue);
        assertEquals(generatorStartValue, dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));

        System.out.println();
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));

        System.out.println();
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        assertEquals(generatorStartValue + 3, dbGenerators.getLastValue(generatorName));

        System.out.println();
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));

        System.out.println();
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        assertEquals(generatorStartValue + 6, dbGenerators.getLastValue(generatorName));

        System.out.println();
        System.out.println("setLastValue: " + generatorSetLastValue);
        dbGenerators.setLastValue(generatorName, generatorSetLastValue);

        System.out.println();
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        System.out.println("genNextValue: " + dbGenerators.genNextValue(generatorName));
        assertEquals(generatorSetLastValue + 3, dbGenerators.getLastValue(generatorName));

        System.out.println();
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        assertEquals(generatorSetLastValue + 3, dbGenerators.getLastValue(generatorName));

        System.out.println();
        System.out.println("dropGenerator");
        dbGenerators.dropGenerator(generatorName);

        try {
            System.out.println("getLastValue: " + dbGenerators.getLastValue(generatorName));
        } catch (Exception e) {
            System.out.println("error getLastValue():");
            System.out.println(UtJdxErrors.collectExceptionText(e));
        }
    }

}
