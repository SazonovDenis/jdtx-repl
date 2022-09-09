package jdtx.repl.main.api.data_filler

import jandcode.dbm.data.*
import jandcode.utils.*
import jdtx.repl.main.api.*
import org.junit.*

class DataFiller_db_Test extends ReplDatabaseStruct_Test {

    public Map<String, String> xxx = [
            "Lic"   : [
                    "ins": [
                            count: 10,
                    ],
                    "upd": [
                            count : 3,
                            values: [
                                    "name" : "**-test-***",
                                    "state": [2, 3, 7]
                            ]
                    ],
                    "upd": [
                            ids: [383, 1496, 2042]
                    ]
            ],
            "ULZ"   : "000",
            "REGION": "000",
    ]

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

        //
        String sql = sqlCheck.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_gen_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db, struct)

        //
        Map<Long, Map> ulz = writer.ins("Ulz", 3)

        licTml.put("ulz", ulz.keySet())
        Map<Long, Map> lic = writer.ins("Lic", 7, licTml)

        pawnChitTml.put("lic", lic.keySet());
        Map<Long, Map> pawnChit = writer.ins("PawnChit", 10, pawnChitTml)

        for (Long pawnChitId : pawnChit.keySet()) {
            PawnChitSubjectTml.put("pawnChit", pawnChitId)
            writer.ins("PawnChitSubject", 1 + rnd.nextInt(3), PawnChitSubjectTml)
        }

        //
        String pawnChitIds = pawnChit.keySet().toString()
        pawnChitIds = pawnChitIds.substring(1, pawnChitIds.length() - 1)
        sql = sqlCheck.replace("#{where}", "and PawnChit.id in (" + pawnChitIds + ")")
        DataStore st1 = db.loadSql(sql)
        DataFiller_gen_Test.UtData_outTable(st1, 999)

        //
        sql = sqlCheck.replace("#{where}", "")
        st = db.loadSql(sql)
        DataFiller_gen_Test.UtData_outTable(st, 999)
    }

    @Test
    void test_ins_Lic() {
        int count = 10

        //
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)
        DataFiller_gen_Test.UtData_outTable(st, 999)

        //
        IDataWriter writer = new DataWriter(db, struct)

        Map<Long, Map> lic = writer.ins("Lic", count, licTml)

        //
        sql = sqlCheckLic.replace("#{where}", "")
        DataStore st1 = db.loadSql(sql)
        DataFiller_gen_Test.UtData_outTable(st1, 999)
    }

    @Test
    void test33() {
        String sql = sqlCheckLic.replace("#{where}", "")
        DataStore st = db.loadSql(sql)

        DataRecord rec = st.get(0)
        String id = rec.getValueString("lic")
        String s = rec.getValueString("NameF")

        UtData.outTable(st, 999)
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

    @Test
    void test1() throws Exception {
        DataFiller filler = new DataFiller(db, struct)

        DataStore st1 = db.loadSql("select * from PawnChit order by id")
        UtData.outTable(st1)

        Map<DataFillerPK, DataFillerRec> ins1 = filler.ins("PawnChit", 2)

        DataStore st2 = db.loadSql("select * from PawnChit order by id")
        UtData.outTable(st2)
    }


/*
     :с ошибки СРАВНЕНИЯ равенства баз

Попытка синхронизации была неудачная
compare: d:/z_disk/jdtx-repl/_test-data/_test-data_srv/db1.gdb
     vs: d:/z_disk/jdtx-repl/_test-data/_test-data_ws2/db2.gdb
2022.05.19 10:05:23,626 INFO   ws  Workstation    dataRoot: Z:\jdtx-repl\_test-data\_test-data_srv/
2022.05.19 10:05:23,638 INFO   ws  Workstation    wsId: 1
2022.05.19 10:05:24,892 INFO   ws  Workstation    dataRoot: Z:\jdtx-repl\_test-data\_test-data_ws2/
2022.05.19 10:05:24,903 INFO   ws  Workstation    wsId: 2
USRLOG, found new in 2
  2:1366
  2:1367
  2:1375
  2:1364
  2:1365
  2:1368
  2:1369
  2:1370
  2:1373
  2:1374
  2:1371
  2:1372
ULZ: not found new in 1
ULZ, found new in 2
  2:4646
  2:4647
  2:4648
  2:4649
  2:4644
  2:4645
REGION: not found new in 1
REGION, found new in 2
  2:1014
  2:1015
  2:1018
  2:1019
  2:1016
  2:1017
====[ ERROR ]===============================================================
Обнаружена разница expected:<false> but was:<true>
----[ filtered stack ]------------------------------------------------------
java.lang.AssertionError: Обнаружена разница expected:<false> but was:<true>
	at jdtx.repl.main.api.ReplDatabaseStruct_Test.compareDb(ReplDatabaseStruct_Test.java:217)
	at jdtx.repl.main.api.JdxReplWsSrv_RestoreWs_DbRestore_test.doLife_AfterFail(JdxReplWsSrv_RestoreWs_DbRestore_test.java:383)
	at jdtx.repl.main.api.JdxReplWsSrv_RestoreWs_DbRestore_test.test_Db2_Dir1(JdxReplWsSrv_RestoreWs_DbRestore_test.java:190)
============================================================================

java.lang.AssertionError: Обнаружена разница
Expected :false
Actual   :true

*/

}
