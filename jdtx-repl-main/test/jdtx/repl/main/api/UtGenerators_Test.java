package jdtx.repl.main.api;

import org.junit.*;

/**
 */
public class UtGenerators_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_compare() throws Exception {
        logOff();
        //
        UtGenerators utGenerators = new UtGenerators_PS(db2, struct2);
        //
        String generatorName = utGenerators.getGeneratorName("ulz");
        System.out.println(generatorName);
        //
        long value = utGenerators.getValue(generatorName);
        System.out.println(value);
        //
        utGenerators.setValue(generatorName, utGenerators.getValue(generatorName) + 1);
        System.out.println(utGenerators.getValue(generatorName));
        assertEquals(value + 1, utGenerators.getValue(generatorName));
        //
        utGenerators.setValue(generatorName, utGenerators.getValue(generatorName) - 1);
        System.out.println(utGenerators.getValue(generatorName));
        assertEquals(value, utGenerators.getValue(generatorName));
        //
        utGenerators.repairGenerator(struct.getTable("ulz"));
        System.out.println(utGenerators.getValue(generatorName));
        assertEquals(value, utGenerators.getValue(generatorName));
    }


}
