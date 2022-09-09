package jdtx.repl.main.api.data_filler


import jandcode.utils.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.struct.IJdxField
import org.junit.*

class FieldValueGenerator_Test extends ReplDatabaseStruct_Test {

    HashMapNoCase<Object> licTml = [
            "nameF": new FieldValueGenerator_String("nF-*********ов"),
            "nameI": new FieldValueGenerator_String("nI-******"),
            "nameO": ["", new FieldValueGenerator_String("nO-*********ич")]
    ]

    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()
    }

    @Test
    void test() {
        println("nameF")
        Object generator = licTml.get("nameF")
        IJdxField field =  struct.getTable("Lic").getField("nameF")
        for (int i = 0; i < 100; i++) {
            println(generator.genValue(field))
        }

        println("nameI")
        generator = licTml.get("nameI")
        field =  struct.getTable("Lic").getField("nameI")
        for (int i = 0; i < 100; i++) {
            println(generator.genValue(field))
        }
    }
}
