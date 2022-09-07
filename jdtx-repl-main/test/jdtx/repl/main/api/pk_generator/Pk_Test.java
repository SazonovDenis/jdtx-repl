package jdtx.repl.main.api.pk_generator;

import jdtx.repl.main.api.*;
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
        IDbGenerators generator1 = DbToolsService.getDbGenerators(db);
        IDbGenerators generator3 = DbToolsService.getDbGenerators(db3);

        IAppPkRules appPkRules1 = app.service(AppPkRulesService.class).createPkGenerator(db, struct);
        IAppPkRules appPkRules3 = app.service(AppPkRulesService.class).createPkGenerator(db3, struct3);

        //
        System.out.println();
        System.out.println("Db 1:");
        test_pk(generator1, appPkRules1);

        //
        System.out.println();
        System.out.println("Db 3:");
        test_pk(generator3, appPkRules3);
    }

    @Test
    public void testSvc() throws Exception {
        IDbGenerators dbGenerators = DbToolsService.getDbGenerators(db2);
        IAppPkRules appPkRules = app.service(AppPkRulesService.class).createPkGenerator(db2, struct2);

        System.out.println("dbGenerators: " + dbGenerators);
        System.out.println("appPkRules: " + appPkRules);

        String tableName = "Lic";
        System.out.println(tableName + ".generator.name: " + appPkRules.getGeneratorName(tableName));
        System.out.println(tableName + ".generator.value: " + dbGenerators.getGeneratorCurrValue(appPkRules.getGeneratorName(tableName)));
    }

    void test_pk(IDbGenerators generator, IAppPkRules appPkRules) throws Exception {
        String tableName = "Ulz";
        String generatorName = appPkRules.getGeneratorName(tableName);
        System.out.println("table: " + tableName + ", generator: " + generatorName);
        //
        long value = generator.getGeneratorCurrValue(generatorName);
        System.out.println("      now: " + value);
        //
        generator.setGeneratorValue(generatorName, generator.getGeneratorCurrValue(generatorName) + 1);
        System.out.println("  forvard: " + generator.getGeneratorCurrValue(generatorName));
        assertEquals(value + 1, generator.getGeneratorCurrValue(generatorName));
        //
        generator.setGeneratorValue(generatorName, generator.getGeneratorCurrValue(generatorName) - 1);
        System.out.println("     back: " + generator.getGeneratorCurrValue(generatorName));
        assertEquals(value, generator.getGeneratorCurrValue(generatorName));
        //
        generator.setGeneratorValue(generatorName, 0);
        System.out.println("   broken: " + generator.getGeneratorCurrValue(generatorName));
        assertEquals(0, generator.getGeneratorCurrValue(generatorName));
        //
        UtPkGeneratorRepair generatorRepair = new UtPkGeneratorRepair(db, struct);
        //
        value = generatorRepair.getMaxPkValue(struct.getTable(tableName));
        System.out.println("    maxPk: " + value);
        //
        generatorRepair.repairGenerator(struct.getTable(tableName));
        System.out.println(" repaired: " + generator.getGeneratorCurrValue(generatorName));
        assertEquals(value, generator.getGeneratorCurrValue(generatorName));
    }


}
