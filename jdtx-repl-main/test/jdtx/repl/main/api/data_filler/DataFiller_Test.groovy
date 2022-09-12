package jdtx.repl.main.api.data_filler


import jandcode.dbm.data.*
import jandcode.dbm.db.Db
import jandcode.utils.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.struct.*
import org.junit.*

class DataFiller_Test extends ReplDatabaseStruct_Test {


    @Override
    void setUp() throws Exception {
        rootDir = "../../ext/"
        super.setUp()
    }


    @Test
    void test_genRec_Ulz() throws Exception {
        int count = 15

        //
        DataFiller filler = new DataFiller(db, struct)

        // Набор генераторов для некоторых полей
        HashMapNoCase<Object> generators = [
                "name"  : ["vvv",
                           new FieldValueGenerator_String("***-test-***", "йцукен12345"),
                           new FieldValueGenerator_String("ch-********************", 20)
                ],
                "UlzTip": [101000003, 101000001, 0],
                "Region": 101000117,
                "state" : [2, 3, 7]
        ]

        // Нагенерим записей по шаблонам
        DataStore st = db.loadSql("select * from Ulz where 1 = 0")
        for (int i = 0; i < count; i++) {
            Map recValues = filler.genRecord(struct.getTable("Ulz"), generators)
            recValues.put("id", i + 1000)
            st.add(recValues)
        }
        UtData.outTable(st)
    }


    @Test
    void test_genRec_PawnChit() throws Exception {
        int count = 15

        // Посмотрим, как сейчас в БД
        DataStore st1 = db.loadSql("select * from PawnChit order by id")
        UtData.outTable(st1, count)

        //
        DataFiller filler = new DataFiller(db, struct)

        // Подготовим набор допустимых значений для ссылочного поля
        Set licSet = UtData.uniqueValues(st1, "Lic")
        licSet.remove(0)

        // Набор генераторов для некоторых полей
        HashMapNoCase<Object> generators = [
                "ChitNo"       : new FieldValueGenerator_String("NO-*****", "0123456789"),
                "ValuationSumm": new FieldValueGenerator_Number(5000, 120000, 1),
                "Storn"        : [0, 0, 0, 1],
                "Lic"          : licSet
        ]

        // Нагенерим записей по шаблонам
        DataStore st = db.loadSql("select * from PawnChit where 1 = 0")
        for (int i = 0; i < count; i++) {
            Map recValues = filler.genRecord(struct.getTable("PawnChit"), generators)
            recValues.put("id", i + 1000)
            st.add(recValues)
        }
        UtData.outTable(st)
    }


    @Test
    void test_genRec_createGenerators_PawnChit() throws Exception {
        int count = 15

        // Посмотрим, как сейчас в БД
        DataStore st1 = db.loadSql("select * from PawnChit order by id")
        UtData.outTable(st1, count)

        //
        DataFiller filler = new DataFiller(db, struct)

        // Набор генераторов для некоторых полей
        HashMapNoCase<Object> generatorsDefault = [
                "ChitNo"       : new FieldValueGenerator_String("NO-*****", "0123456789"),
                "ValuationSumm": new FieldValueGenerator_Number(5000, 120000, 1),
                "Storn"        : [0, 0, 0, 1]
        ]

        // Нагенерим генераторов для остальных полей в таблице
        Map<String, Object> generators = filler.createGenerators(struct.getTable("PawnChit"), generatorsDefault)

        // Нагенерим записей по шаблонам
        DataStore st2 = db.loadSql("select * from PawnChit where 1 = 0")
        for (int i = 0; i < count; i++) {
            Map recValues = filler.genRecord(struct.getTable("PawnChit"), generators)
            recValues.put("id", i + 1000)
            st2.add(recValues)
        }
        UtData.outTable(st2)
    }


    @Test
    void test_genRec_createGenerators_PawnChitDat() throws Exception {
        DataFiller filler = new DataFiller(db, struct)
        doTable(db, struct.getTable("PAWNCHITDAT"), filler)
    }


    @Test
    void test_genRec_createGenerators_All() throws Exception {
        DataFiller filler = new DataFiller(db, struct)

        for (IJdxTable table : struct.tables) {
            println("table: " + table.getName())

            doTable(db, table, filler)

            println()
        }
    }


    void doTable(Db db, IJdxTable table, DataFiller filler) throws Exception {
        int count = 15

        //
        if (table.getPrimaryKey().size() == 0) {
            println("table has no PK")
            return
        }
        IJdxField pkField = table.getPrimaryKey().get(0)
        String pkFieldName = pkField.getName()

        // Посмотрим, как сейчас в БД
        DataStore st1 = db.loadSql("select * from " + table.getName() + " order by " + pkFieldName)
        UtData_outTable(st1, count)

        // Набор генераторов для некоторых полей (например, id пусть будет null)
        Map generatorsDefault = UtCnv.toMap(pkFieldName, null)

        // Нагенерим генераторов для остальных полей в таблице
        Map<String, Object> generators = filler.createGenerators(table, generatorsDefault)

        // Нагенерим записей по шаблонам
        DataStore st2 = db.loadSql("select * from " + table.getName() + " where 1 = 0")
        for (int i = 0; i < count; i++) {
            Map recValues = filler.genRecord(table, generators)
            st2.add(recValues)
        }
        UtData_outTable(st2, count)
    }


    public static void UtData_outTable(DataStore st, int limit) {
        OutTableSaver sv = new OutTableSaver_Dvsa(st)
        sv.setMaxColWidth(35)
        sv.setLimit(limit)
        String s = sv.save().toString()
        System.out.println(s)
    }


}
