package jdtx.repl.main.api.data_filler

import jandcode.dbm.data.*
import jandcode.dbm.db.*
import jandcode.utils.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.struct.*
import org.junit.*

class DataWriter_Oracle_Test extends ReplDatabaseStruct_Test {

    HashMapNoCase<Object> fileItemTml = [
            "parent"      : 1,
            "LINKFILEITEM": null,
            "DocNo"       : new FieldValueGenerator_String("************", "0123456789"),
            "name"        : new FieldValueGenerator_String("File*********", 15)
    ]

    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()
    }

    @Test
    void test_ins_FileItem() {
        int count = 10

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckFileItem.replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db_one, struct_one)

        // Добавим
        Map<Long, Map> set = writer.ins("FileItem", count, fileItemTml)
        println("inserted: " + set.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckFileItem.replace("#{where}", "")
        DataStore st1 = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test_ins_FileDict() {
        int count = 10

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckFileDict.replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        UtData.outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db_one, struct_one)

        // Добавим
        Map<Long, Map> set = writer.ins("FileDict", count)
        println("inserted: " + set.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckFileDict.replace("#{where}", "")
        st = db_one.loadSql(sql)
        UtData.outTable(st, 999)
    }

    @Test
    void test_load_FileLog() {
        // Посмотрим, как сейчас в БД
        String sql = sqlCheck.
                replace("#{table}", "fileLog").
                replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 5)
    }

    @Test
    void test_ins_FileLog() {
        DataWriter writer = new DataWriter(db_one, struct_one)
        doTable(db_one, struct_one.getTable("FileLog"), writer)
    }

    @Test
    void test_ins_All() {
        DataWriter writer = new DataWriter(db_one, struct_one)

        for (IJdxTable table : struct_one.tables) {
            println("table: " + table.getName())

            doTable(db_one, table, writer)

            println()
        }
    }

    @Test
    void test_del_FileItem() {
        int count = 5

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckFileItem.replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db_one, struct_one)

        // Получим все id
        DataStore st1 = db_one.loadSql("select id from FileItem")
        Set set = UtData.uniqueValues(st1, "id")
        set.remove(0)
        set.remove(1)

        // Отберем из них несколько
        Set setDel = writer.choiceSubsetFromSet(set, count)

        // Удалим отобранные id
        Map<Long, Map> setRes = writer.del("FileItem", setDel, true)
        println("deleted: " + setRes.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckFileItem.replace("#{where}", "")
        st1 = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test_print_FileItem() {
        String sql = sqlCheckFileItem.replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }


    String sqlCheckFileItem = """
select
  FileItem.*
from
  FileItem
where
  1 = 1 #{where}
order by
  FileItem.id
"""

    String sqlCheckFileDict = """
select
  FileDict.*
from
  FileDict
where
  1 = 1 #{where}
order by
  FileDict.id
"""


    void doTable(Db db, IJdxTable table, DataWriter writer) {
        int count = 30

        String tableName = table.getName()

        // Посмотрим, как сейчас в БД
        String sql = sqlCheck.
                replace("#{table}", tableName).
                replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 5)

        // 
        HashMapNoCase<Object> generatorsDefault = new HashMapNoCase<>()
        generatorsDefault.put("field:fileItem.ISFOLDER", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:fileLog.fileItemIDE", new FieldValueGenerator_Number(0, 3, 0))
        generatorsDefault.put("field:fileLog.fileAttrIDE", new FieldValueGenerator_Number(0, 3, 0))
        generatorsDefault.put("field:fileItemOpr.ReplacePolitic", new FieldValueGenerator_Number(0, 3, 0))
        generatorsDefault.put("field:WAXAUTH_USER.LOCKED", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FILEATTRDEF.ISSYS", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FILEATTRDEF.MULTI", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FILEATTRDEF.USECOMBO", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.MULTI", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.ISREQ", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.ISAUTOFILL", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FileStorage.FORUPLOAD", new FieldValueGenerator_Number(0, 1, 0))

        // Добавим
        Map<Long, Map> set = writer.ins(tableName, count, generatorsDefault)
        println("inserted: " + set.keySet())

        // Посмотрим, как сейчас в БД
        String idsStr = set.keySet().toString()
        idsStr = idsStr.substring(1, idsStr.length() - 1)
        sql = sqlCheck.
                replace("#{table}", tableName).
                replace("#{where}", "and " + tableName + ".id in (" + idsStr + ")")
        //
        st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    String sqlCheck = """
select
  #{table}.*
from
  #{table}
where
  1 = 1 #{where}
order by
  #{table}.id
"""

}
