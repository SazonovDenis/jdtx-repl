package jdtx.repl.main.api;

import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;

/**
 */
public class UtTest extends UtilsTestCase {

    public void compareStruct(IJdxDbStruct struct_1, IJdxDbStruct struct_2) {
        assertEquals("Количество таблиц", struct_1.getTables().size(), struct_2.getTables().size());
        for (int t = 0; t < struct_1.getTables().size(); t++) {
            assertEquals("Количество полей в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().size(), struct_2.getTables().get(t).getFields().size());
            for (int f = 0; f < struct_1.getTables().get(t).getFields().size(); f++) {
                assertEquals("Полея в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().get(f).getName(), struct_2.getTables().get(t).getFields().get(f).getName());
            }
        }
    }


    public void makeChange(String tableName){

    }
}
