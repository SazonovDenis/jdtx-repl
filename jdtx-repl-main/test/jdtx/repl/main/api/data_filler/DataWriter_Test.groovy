package jdtx.repl.main.api.data_filler

import jandcode.dbm.data.*
import jandcode.utils.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.struct.*
import org.junit.*

class DataWriter_Test extends ReplDatabaseStruct_Test {

    HashMapNoCase<Object> pawnChitTml = [
            "chitNo": new FieldValueGenerator_String("N-****", "0123456789"),
            "creditSumm": new FieldValueGenerator_Number(20000, 100000, 0),
    ]

    HashMapNoCase<Object> PawnChitSubjectTml = [
            "subjectNo": new FieldValueGenerator_String("**", "0123456789"),
            "creditSumm": new FieldValueGenerator_Number(2000, 50000, 0),
            "valuationSumm": new FieldValueGenerator_Number(9000, 80000, 0)
    ]

    HashMapNoCase<Object> licTml = [
            "DocNo": new FieldValueGenerator_String("************", "0123456789"),
            "nameF": new FieldValueGenerator_String("F*********ов", 15),
            "nameI": new FieldValueGenerator_String("I******", 15),
            "nameO": ["", new FieldValueGenerator_String("O*********ич", 15)]
    ]

    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()
    }

    @Test
    void test_ins() {
        Random rnd = new Random();

        // Посмотрим, как сейчас в БД
        String sql = sqlCheck.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 10)

        //
        IDataWriter writer = new DataWriter(db, struct)


        // Добавим взаимозависимые записи
        Map<Long, Map> ulz = writer.ins("Ulz", 3)
        println("inserted Ulz: " + ulz.keySet())

        licTml.put("ulz", ulz.keySet())
        Map<Long, Map> lic = writer.ins("Lic", 7, licTml)
        println("inserted Lic: " + lic.keySet())

        pawnChitTml.put("lic", lic.keySet());
        Map<Long, Map> pawnChit = writer.ins("PawnChit", 10, pawnChitTml)
        println("inserted PawnChit: " + pawnChit.keySet())

        for (Long pawnChitId : pawnChit.keySet()) {
            Map<Long, Map> pawnChitSubject = new HashMap<>()
            int subjectNoCount = (1 + rnd.nextInt(3))
            for (int subjectNo = 1; subjectNo <= subjectNoCount; subjectNo++) {
                PawnChitSubjectTml.put("pawnChit", pawnChitId)
                PawnChitSubjectTml.put("subjectNo", subjectNo)
                pawnChitSubject.putAll(writer.ins("PawnChitSubject", 1, PawnChitSubjectTml))
            }
            println("inserted PawnChitSubject: " + pawnChitSubject.keySet())
        }


        // Посмотрим, как сейчас в БД
        sql = sqlCheck.replace("#{where}", "and " + "pawnChit" + ".id in (" + setToStr(pawnChit.keySet()) + ")")
        st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    @Test
    void test_ins_Lic() {
        int count = 10

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db, struct)

        // Добавим
        Map<Long, Map> lic = writer.ins("Lic", count, licTml)
        println("inserted: " + lic.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckLic.replace("#{where}", "")
        DataStore st1 = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test_del_Lic() {
        int count = 3

        // Посмотрим, как сейчас в БД
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db, struct)

        // Получим все id
        Set<Long> setFull = writer.utFiller.loadAllIds("Lic")
        setFull.remove(0)

        // Отберем из них несколько
        Set setDel = writer.utFiller.choiceSubsetFromSet(setFull, count)

        // Удалим отобранные id
        writer.del("Lic", setDel, true)
        println("deleted: " + setDel)

        // Посмотрим, как сейчас в БД
        sql = sqlCheckLic.replace("#{where}", "")
        DataStore st1 = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test_print_lic() {
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    @Test
    void test_ins_PAWNCHITOPR() {
        int count = 30

        String tableName = "PAWNCHITOPR"

        DataWriter writer = new DataWriter(db, struct, null)

        ins_Table(tableName, writer, count)
    }

    @Test
    void test_print_PAWNCHITOPR() {
        String tableName = "PAWNCHITOPR"

        String sql = sqlCheckTable.
                replace("#{table}", tableName).
                replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    @Test
    void test_print_CALC_PAWNCHITSUBJECT() {
        String tableName = "CALC_PAWNCHITSUBJECT"

        String sql = sqlCheckTable.
                replace("#{table}", tableName).
                replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    @Test
    void test_ins_All() {
        int count = 30

        // Мешает, т.к. нет генераторов и PK кривой
        struct.tables.remove(struct.getTable("KSRTDB_DICTONARYVERSION"))
        struct.tables.remove(struct.getTable("WAX_MODULEVERDB"))
        struct.tables.remove(struct.getTable("CALC_SUBJECTOPR"))
        struct.tables.remove(struct.getTable("CALC_DATE"))
        struct.tables.remove(struct.getTable("CALC_PAWNCHITSUBJECT"))
        // Мешает, т.к. триггер рассчитывает на ЛОГИЧЕСКУЮ целостность (одна OPR - одна PAWNCHITOPR), а тут она нарушается
        struct.tables.remove(struct.getTable("PAWNCHITOPR"))

        //
        DataWriter writer = new DataWriter(db, struct, null)
        //generatorsDefault.put("field:fileItem.linkFileItem", [null, null, null, null, null, new FieldValueGenerator_Ref(db_one, struct_one, writer.filler)])

        //
        for (IJdxTable table : struct.tables) {
            String tableName = table.getName()
            println("table: " + tableName)

            //
            if (table.getPrimaryKey().size() == 0) {
                println("table: " + tableName + " has no PK");
                continue;
            }

            ins_Table(tableName, writer, count)

            println()
        }
    }

    @Test
    void test_del_All() {
        int count = 50

        // Мешает, т.к. нет генераторов и PK кривой
        struct.tables.remove(struct.getTable("KSRTDB_DICTONARYVERSION"))
        struct.tables.remove(struct.getTable("WAX_MODULEVERDB"))
        struct.tables.remove(struct.getTable("CALC_SUBJECTOPR"))
        struct.tables.remove(struct.getTable("CALC_DATE"))
        struct.tables.remove(struct.getTable("CALC_PAWNCHITSUBJECT"))
        // Мешает, т.к. триггер рассчитывает на ЛОГИЧЕСКУЮ целостность (одна OPR - одна PAWNCHITOPR), а тут она нарушается
        struct.tables.remove(struct.getTable("PAWNCHITOPR"))

        //
        IDataWriter writer = new DataWriter(db, struct)

        //
        for (IJdxTable table : struct.tables) {
            String tableName = table.getName()
            println("table: " + tableName)

            //
            if (table.getPrimaryKey().size() == 0) {
                println("table: " + tableName + " has no PK");
                continue;
            }

            del_Table(tableName, writer, count)

            println()
        }
    }

    void del_Table(String tableName, DataWriter writer, int count) {
        // Посмотрим, как сейчас в БД
        //String sql = sqlCheckTable.
        //        replace("#{table}", tableName).
        //        replace("#{where}", "")
        //DataStore st = db_one.loadSql(sql)
        //DataFiller_Test.UtData_outTable(st, 15)

        // Получим все id
        Set<Long> setFull = writer.utFiller.loadAllIds(tableName)
        setFull.remove(0L)
        setFull.remove(1L)

        // Отберем из них несколько
        Set<Long> setDel = writer.utFiller.choiceSubsetFromSet(setFull, 1000, Long.MAX_VALUE, count)

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

    void ins_Table(String tableName, DataWriter writer, int count) {
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
        DataStore st1 = writer.db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 3)
    }

    String setToStr(Set set) {
        String idsStr = set.toString()
        idsStr = idsStr.substring(1, idsStr.length() - 1)
        return idsStr
    }


    String sqlCheck = """
select
  PawnChit.id pawnChit,
  PawnChitSubject.id pawnChitSubject,
  PawnChit.chitNo || '/' || PawnChitSubject.subjectNo as subjectNo,
  PawnChit.chitDt,
  PawnChitSubject.valuationSumm,
  PawnChitSubject.creditSumm,
  PawnChitSubject.info,
  Lic.nameF || ' ' || Lic.nameI || ' ' || Lic.nameO as licName,
  Lic.docNo as licDocNo,
  Ulz.name as ulz, 
  Region.name as region
from
  PawnChitSubject
  left join PawnChit on (PawnChitSubject.PawnChit = PawnChit.id)
  left join Lic on (PawnChit.Lic = Lic.id)
  left join Ulz on (Lic.Ulz = Ulz.id)
  left join Region on (Ulz.Region = Region.id)
where
  PawnChitSubject.id <> 0 #{where}
order by
  PawnChit.id, PawnChitSubject.id,
  PawnChit.chitNo,
  PawnChitSubject.subjectNo
"""

    String sqlCheckLic = """
select
  Lic.id as lic,
  Lic.nameF,
  Lic.nameI,
  Lic.nameF || ' ' || Lic.nameI || ' ' || Lic.nameO as licName,
  Lic.docNo as licDocNo,
  Ulz.name as ulz, 
  Region.name as region
from
  Lic
  left join Ulz on (Lic.Ulz = Ulz.id)
  left join Region on (Ulz.Region = Region.id)
where
  Lic.id <> 0 #{where}
order by
  Lic.id
"""


}
