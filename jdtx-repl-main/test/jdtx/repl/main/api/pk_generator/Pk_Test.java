package jdtx.repl.main.api.pk_generator;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

/**
 *
 */
public class Pk_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        //
        super.setUp();
    }

    @Test
    public void test_pk_1_3() throws Exception {
        logOff();

        //
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        //
        RefManagerService refManager = app.service(RefManagerService.class);
        refManager.init(db, ws);

        //
        IDbGenerators generator1 = db.service(DbGeneratorsService.class);
        IDbGenerators generator3 = db3.service(DbGeneratorsService.class);

        IAppPkRules appPkRules1 = app.service(AppPkRulesService.class);
        IAppPkRules appPkRules3 = app.service(AppPkRulesService.class);

        //
        System.out.println();
        System.out.println("Db 1, " + UtJdx.getDbInfoStr(db));
        test_pk(generator1, appPkRules1, refManager);

        //
        System.out.println();
        System.out.println("Db 3, " + UtJdx.getDbInfoStr(db3));
        test_pk(generator3, appPkRules3, refManager);
    }

    void test_pk(IDbGenerators dbGenerators, IAppPkRules appPkRules, IRefManager refManager) throws Exception {
        System.out.println("dbGenerators: " + dbGenerators.getClass());
        System.out.println("appPkRules: " + appPkRules.getClass());
        System.out.println("refManager: " + refManager.getClass());

        String tableName = "Ulz";
        String generatorName = appPkRules.getGeneratorName(tableName);
        System.out.println("table: " + tableName + ", generator: " + generatorName);
        //
        long value = dbGenerators.getLastValue(generatorName);
        System.out.println("      now: " + value);
        //
        dbGenerators.setLastValue(generatorName, dbGenerators.getLastValue(generatorName) + 1);
        System.out.println("  forvard: " + dbGenerators.getLastValue(generatorName));
        assertEquals(value + 1, dbGenerators.getLastValue(generatorName));
        //
        dbGenerators.setLastValue(generatorName, dbGenerators.getLastValue(generatorName) - 1);
        System.out.println("     back: " + dbGenerators.getLastValue(generatorName));
        assertEquals(value, dbGenerators.getLastValue(generatorName));
        //
        dbGenerators.setLastValue(generatorName, 0);
        System.out.println("   broken: " + dbGenerators.getLastValue(generatorName));
        assertEquals(0, dbGenerators.getLastValue(generatorName));
        //
        UtPkGeneratorRepair generatorRepair = new UtPkGeneratorRepair(db, struct);
        //
        value = refManager.get_max_own_id(struct.getTable(tableName));
        System.out.println("    maxPk: " + value);
        //
        generatorRepair.repairGenerator(struct.getTable(tableName));
        System.out.println(" repaired: " + dbGenerators.getLastValue(generatorName));
        assertEquals(value, dbGenerators.getLastValue(generatorName));
    }


}
