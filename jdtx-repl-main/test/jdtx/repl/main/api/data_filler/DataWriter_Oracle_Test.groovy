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

    HashMapNoCase<Object> generatorsDefault = new HashMapNoCase<>()

    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()

        //
        generatorsDefault.put("field:fileItem.linkFileItem", [null])
        generatorsDefault.put("field:fileItem.ISFOLDER", new FieldValueGenerator_Number(0, 1, 0))
        //generatorsDefault.put("field:fileItem.FILEITEMSTATUS", new FieldValueGenerator_Number(0, 50, 0))
        generatorsDefault.put("field:fileItem.state", [null])
        generatorsDefault.put("field:fileItem.FileSize", new FieldValueGenerator_Number(0, 100000, 0))
        generatorsDefault.put("field:fileItem.FileCrc", new FieldValueGenerator_String("****************", "0123456789ABCDEF"))
        generatorsDefault.put("field:fileLog.fileItem", new FieldValueGenerator_Number(0, 30000, 0))
        //generatorsDefault.put("field:fileLog.fileItemIDE", new FieldValueGenerator_Number(0, 3, 0))
        //generatorsDefault.put("field:fileLog.fileAttrIDE", new FieldValueGenerator_Number(0, 3, 0))
        generatorsDefault.put("field:FileLogUsed.lastUsedId", new FieldValueGenerator_Number(0, 30000, 0))
        //generatorsDefault.put("field:fileItemOpr.ReplacePolitic", new FieldValueGenerator_Number(0, 3, 0))
        //generatorsDefault.put("field:WAXAUTH_USER.LOCKED", new FieldValueGenerator_Number(0, 1, 0))
        //generatorsDefault.put("field:WAX_VERDB.ver", new FieldValueGenerator_Number(0, 10, 0))
        generatorsDefault.put("field:FILEATTRDEF.ISSYS", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FILEATTRDEF.MULTI", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FILEATTRDEF.USECOMBO", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.MULTI", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.ISREQ", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FolderAttrTemplate.ISAUTOFILL", new FieldValueGenerator_Number(0, 1, 0))
        generatorsDefault.put("field:FileStorage.FORUPLOAD", new FieldValueGenerator_Number(0, 1, 0))
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
        String sql = sqlCheckTable.
                replace("#{table}", "fileLog").
                replace("#{where}", "")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 5)
    }

    @Test
    void test_ins_FileLog() {
        int count = 30

        //
        DataWriter writer = new DataWriter(db_one, struct_one, generatorsDefault)

        //
        ins_Table(db_one, struct_one.getTable("FileLog"), writer, count)
    }

    @Test
    void test_ins_All() {
        int count = 30

        //
        DataWriter writer = new DataWriter(db_one, struct_one, generatorsDefault)
        generatorsDefault.put("field:fileItem.linkFileItem", [null, null, null, null, null, new FieldValueGenerator_Ref(db_one, struct_one, writer.filler)])

        //
        for (IJdxTable table : struct_one.tables) {
            println("table: " + table.getName())

            ins_Table(db_one, table, writer, count)

            println()
        }
    }

    @Test
    void test_del_FileItem() {
        int count = 50
        String tableName = "FileItem"
        del_Table(tableName, count)
    }

    @Test
    void test_del_FileAttrDef() {
        int count = 5
        String tableName = "FileAttrDef"
        del_Table(tableName, count)
    }

    @Test
    void test_del_All() {
        int count = 50

        for (IJdxTable table : struct_one.tables) {
            String tableName = table.getName()

            println("table: " + tableName)

            del_Table(tableName, count)

            println()
        }
    }

    void del_Table(String tableName, int count) {
        // Посмотрим, как сейчас в БД
        //String sql = sqlCheckTable.
        //        replace("#{table}", tableName).
        //        replace("#{where}", "")
        //DataStore st = db_one.loadSql(sql)
        //DataFiller_Test.UtData_outTable(st, 15)

        //
        IDataWriter writer = new DataWriter(db_one, struct_one)

        // Получим все id
        Set<Long> setFull = writer.utFiller.loadAllIds(tableName)
        setFull.remove(0L)
        setFull.remove(1L)

        // Отберем из них несколько
        Set<Long> setDel = writer.utFiller.choiceSubsetFromSet(setFull, count)

        // Удалим отобранные id
        println("deleting: " + setDel)
        writer.del(tableName, setDel, true)

        // Посмотрим, как сейчас в БД
        //sql = sqlCheckTable.
        //        replace("#{table}", tableName).
        //        replace("#{where}", "")
        //DataStore st1 = db_one.loadSql(sql)
        //DataFiller_Test.UtData_outTable(st1, 15)
    }

    @Test
    void test_upd_FileItem() {
        int count = 5
        String tableName = "FileItem"

        //
        IDataWriter writer = new DataWriter(db_one, struct_one, generatorsDefault)

        // Получим все id
        Set<Long> setFull = writer.utFiller.loadAllIds(tableName)
        setFull.remove(0)
        setFull.remove(1)

        // Отберем из них сколько просили
        Set<Long> setUpd = writer.utFiller.choiceSubsetFromSet(setFull, count)

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckFileItem.
                replace("#{table}", tableName).
                replace("#{where}", "and " + tableName + ".id in (" + setToStr(setUpd) + ")")
        DataStore st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)

        // Апдейтим отобранные id
        Map<Long, Map> updRes = writer.upd(tableName, setUpd)
        println("updated: " + updRes.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckFileItem.
                replace("#{table}", tableName).
                replace("#{where}", "and " + tableName + ".id in (" + setToStr(updRes.keySet()) + ")")
        st = db_one.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
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

    String sqlCheckTable = """
select
  #{table}.*
from
  #{table}
where
  1 = 1 #{where}
order by
  #{table}.id
"""

    void ins_Table(Db db, IJdxTable table, DataWriter writer, int count) {
        String tableName = table.getName()

        // Посмотрим, как сейчас в БД
        //String sql = sqlCheckTable.
        //        replace("#{table}", tableName).
        //        replace("#{where}", "")
        //DataStore st = db.loadSql(sql)
        //DataFiller_Test.UtData_outTable(st, 5)

        // Добавим
        Map<Long, Map> set = writer.ins(tableName, count)
        println("inserted: " + set.keySet())

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckTable.
                replace("#{table}", tableName).
                replace("#{where}", "and " + tableName + ".id in (" + setToStr(set.keySet()) + ")")
        //
        DataStore st1 = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 3)
    }

    String setToStr(Set set) {
        String idsStr = set.toString()
        idsStr = idsStr.substring(1, idsStr.length() - 1)
        return idsStr
    }

}
