package jdtx.repl.main.api.data_filler

import jandcode.dbm.data.*
import jandcode.utils.*
import jdtx.repl.main.api.*
import org.junit.*

class DataWriter_Test extends ReplDatabaseStruct_Test {

    HashMapNoCase<Object> pawnChitTml = [
            "chitNo": new FieldValueGenerator_String("N-****", "0123456789")
    ]

    HashMapNoCase<Object> PawnChitSubjectTml = [
            "subjectNo": new FieldValueGenerator_String("**", "0123456789")
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
        DataFiller_Test.UtData_outTable(st, 999)

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
            PawnChitSubjectTml.put("pawnChit", pawnChitId)
            Map<Long, Map> PawnChitSubject = writer.ins("PawnChitSubject", 1 + rnd.nextInt(3), PawnChitSubjectTml)
            println("inserted PawnChitSubject: " + PawnChitSubject.keySet())
        }

        //
        // String idsStr = pawnChit.keySet().toString()
        // idsStr = idsStr.substring(1, idsStr.length() - 1)
        // sql = sqlCheck.replace("#{where}", "and PawnChit.id in (" + idsStr + ")")
        // DataStore st1 = db.loadSql(sql)
        // DataFiller_gen_Test.UtData_outTable(st1, 999)

        // Посмотрим, как сейчас в БД
        sql = sqlCheck.replace("#{where}", "")
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
        DataStore st1 = db.loadSql("select id from lic")
        Set licSet = UtData.uniqueValues(st1, "id")
        licSet.remove(0)

        // Отберем из них несколько
        Set licSetDel = writer.choiceSubsetFromSet(licSet, count)

        // Удалим отобранные id
        Map<Long, Map> lic = writer.del("Lic", licSetDel, true)
        println("deleted: " + lic.keySet())

        // Посмотрим, как сейчас в БД
        sql = sqlCheckLic.replace("#{where}", "")
        st1 = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test_print_lic() {
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_Test.UtData_outTable(st, 999)
    }

    String sqlCheck = """
select
  PawnChit.id pawnChit,
  PawnChitSubject.id pawnChitSubject,
  PawnChit.chitNo || '/' || PawnChitSubject.subjectNo as subjectNo,
  PawnChit.chitDt,
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
