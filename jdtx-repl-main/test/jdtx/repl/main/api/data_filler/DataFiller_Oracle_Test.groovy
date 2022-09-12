package jdtx.repl.main.api.data_filler


import jandcode.dbm.data.*
import jandcode.utils.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.struct.*
import org.junit.*

class DataFiller_Oracle_Test extends DataFiller_Test {


    HashMapNoCase<Object> fileItemTml = [
            "parent": 1,
            "LINKFILEITEM": null,
            "DocNo": new FieldValueGenerator_String("************", "0123456789"),
            "name" : new FieldValueGenerator_String("File*********", 15)
    ]

    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()
    }


    @Test
    void test_gen_FileItem() {
        DataFiller filler = new DataFiller(db_one, struct_one)
        doTable(db_one, struct_one.getTable("FileItem"), filler)
    }


}
