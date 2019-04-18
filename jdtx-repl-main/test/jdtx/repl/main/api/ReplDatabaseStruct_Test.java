package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;

/**
 */
public class ReplDatabaseStruct_Test extends ReplDatabase_Test {

    // Структуры
    public IJdxDbStruct struct;
    public IJdxDbStruct struct2;
    public IJdxDbStruct struct3;

    public void setUp() throws Exception {
        //
        super.setUp();

        // Структуры db и db1
        reloadStruct_forTest();
    }

    // Структуры db и db1
    public void reloadStruct_forTest() throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
        reader.setDb(db2);
        struct2 = reader.readDbStruct();
        reader.setDb(db3);
        struct3 = reader.readDbStruct();
    }

}
