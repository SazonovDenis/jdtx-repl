package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class ReplDatabaseStruct_Test extends ReplDatabase_Test {

    // Структуры
    IJdxDbStruct struct;
    IJdxDbStruct struct2;
    IJdxDbStruct struct3;

    public void setUp() throws Exception {
        //
        super.setUp();

        // Структуры db и db1
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
        reader.setDb(db2);
        struct2 = reader.readDbStruct();
        reader.setDb(db3);
        struct3 = reader.readDbStruct();
    }

}
