package jdtx.repl.main.api;

import org.junit.*;

/**
 */
public class PkGenerator_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_compare() throws Exception {
        logOff();
        //
        IPkGenerator IPkGenerator = new PkGenerator_PS(db2, struct2);
        //
        String generatorName = IPkGenerator.getGeneratorName("ulz");
        System.out.println(generatorName);
        //
        long value = IPkGenerator.getValue(generatorName);
        System.out.println(value);
        //
        IPkGenerator.setValue(generatorName, IPkGenerator.getValue(generatorName) + 1);
        System.out.println(IPkGenerator.getValue(generatorName));
        assertEquals(value + 1, IPkGenerator.getValue(generatorName));
        //
        IPkGenerator.setValue(generatorName, IPkGenerator.getValue(generatorName) - 1);
        System.out.println(IPkGenerator.getValue(generatorName));
        assertEquals(value, IPkGenerator.getValue(generatorName));
        //
        IPkGenerator.repairGenerator(struct.getTable("ulz"));
        System.out.println(IPkGenerator.getValue(generatorName));
        assertEquals(value, IPkGenerator.getValue(generatorName));
    }


}
