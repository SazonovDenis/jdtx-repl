package jdtx.repl.main.api.pk_generator;

import jdtx.repl.main.api.*;
import org.junit.*;

/**
 *
 */
public class PkGenerator_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_compare() throws Exception {
        logOff();
        //
        IPkGenerator IPkGenerator = new PkGenerator_PS(db3, struct2);
        //
        String generatorName = IPkGenerator.getGeneratorName("ulz");
        System.out.println("generator: " + generatorName);
        //
        long value = IPkGenerator.getValue(generatorName);
        System.out.println("      now: " + value);
        //
        IPkGenerator.setValue(generatorName, IPkGenerator.getValue(generatorName) + 1);
        System.out.println("  forvard: " + IPkGenerator.getValue(generatorName));
        assertEquals(value + 1, IPkGenerator.getValue(generatorName));
        //
        IPkGenerator.setValue(generatorName, IPkGenerator.getValue(generatorName) - 1);
        System.out.println("     back: " + IPkGenerator.getValue(generatorName));
        assertEquals(value, IPkGenerator.getValue(generatorName));
        //
        IPkGenerator.setValue(generatorName, 0);
        System.out.println("   broken: " + IPkGenerator.getValue(generatorName));
        assertEquals(0, IPkGenerator.getValue(generatorName));
        //
        IPkGenerator.repairGenerator(struct.getTable("ulz"));
        System.out.println(" repaired: " + IPkGenerator.getValue(generatorName));
        assertEquals(value, IPkGenerator.getValue(generatorName));
    }


}
