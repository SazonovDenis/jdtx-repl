package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 */
public class UtTest extends UtilsTestCase {

    Db db;

    public UtTest(Db db) {
        this.db = db;
    }

    public void dumpTable(String tableName, String outFileName, String sortBy) throws Exception {
        UtFile.mkdirs(outFileName.substring(0, outFileName.length() - UtFile.filename(outFileName).length()));
        //
        String sql = "select * from " + tableName;
        if (sortBy != null) {
            sql = sql + " order by " + sortBy;
        }
        DataStore st = db.loadSql(sql);
        //
        OutTableSaver svr = new OutTableSaver(st);
        svr.save().toFile(outFileName);
    }

    public void changeDbStruct_AddRandomField(String tableName) throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        IJdxTable table = struct.getTable(tableName);
        int lastFieldNo = getLastRandomFieldNo(table);
        String fieldName = "TEST_FIELD_" + (lastFieldNo + 1);
        db.execSql("alter table " + tableName + " add " + fieldName + " varchar(200)");
    }

    public void changeDbStruct_DropFirstRandomField(String tableName) throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        IJdxTable table = struct.getTable(tableName);
        for (IJdxField field : table.getFields()) {
            if (field.getName().startsWith("TEST_FIELD_")) {
                String fieldName = field.getName();
                db.execSql("alter table " + tableName + " drop " + fieldName);
                break;
            }
        }
    }

    public void changeDbStruct_DropLastField(String tableName) throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        IJdxTable table = struct.getTable(tableName);
        IJdxField field = table.getFields().get(table.getFields().size() - 1);
        String fieldName = field.getName();
        db.execSql("alter table " + tableName + " drop " + fieldName);
    }

    public void changeDbStruct_DropTable(String tableName) throws Exception {
        String sql = "drop table " + tableName;
        try {
            db.execSql(sql);
        } catch (Exception e) {
            if (!JdxUtils.errorIs_TableNotExists(e)) {
                throw e;
            }
        }
    }

    public void changeDbStruct_AddRandomTable() throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        //
        int tablesCount = struct.getTables().size();
        String tableName = "TEST_TABLE_" + (tablesCount + 1);
        //
        String sql = "create table " + tableName + " (id integer not null, name varchar(200))";
        db.execSql(sql);
        sql = "alter table " + tableName + " add constraint pk_" + tableName + "_id primary key (id)";
        db.execSql(sql);
        sql = "CREATE generator g_" + tableName;
        db.execSql(sql);

        // Немного повставляем записей
        Random rnd = new Random();
        rnd.setSeed(getDbSeed());
        DbUtils dbu = new DbUtils(db, struct);
        //
        long id = dbu.getNextGenerator("g_" + tableName);
        sql = "insert into " + tableName + " (id, name) values (" + id + ", '" + "new~" + rnd.nextInt() + "')";
        db.execSql(sql);
        //
        id = dbu.getNextGenerator("g_" + tableName);
        sql = "insert into " + tableName + " (id, name) values (" + id + ", '" + "new~" + rnd.nextInt() + "')";
        db.execSql(sql);
    }

    public void makeChangeUnimportant(IJdxDbStruct struct, long ws_id) throws Exception {
        DbUtils dbu = new DbUtils(db, struct);
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        DataRecord rec = db.loadSql("select * from lic where id = " + id01).getCurRec();
        dbu.updateRec("lic",
                UtCnv.toMap("id", id01, "NameF", rec.getValue("NameF")),
                "NameF",
                "NameI,NameO,bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info"
        );
    }

    public void makeChange(IJdxDbStruct struct, long ws_id) throws Exception {
        Random rnd = new Random();
        rnd.setSeed(getDbSeed());
        DbUtils dbu = new DbUtils(db, struct);


        // ---
        long id0 = dbu.getNextGenerator("g_region");
        Map params = UtCnv.toMap(
                "id", id0,
                "regionTip", 1,
                "parent", 0,
                "name", "Name-ins-ws:" + ws_id + "-" + rnd.nextInt()
        );
        //
        String regionTestFields = "";
        for (IJdxField f : struct.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields = regionTestFields + "Region." + f.getName() + ",";
                params.put(f.getName(), f.getName() + "-ins-ws:" + ws_id + "-" + rnd.nextInt());
            }
        }
        //
        dbu.insertRec("region", params);


        // --- DictList
        long id5 = dbu.getNextGenerator("g_DictList");
        dbu.insertRec("DictList", UtCnv.toMap(
                "id", id5,
                "Name", "-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ), "Name");
        // диапазон "старых" значений
        long id5_01 = db.loadSql("select min(id) id from DictList where id > 0 and id < 1000").getCurRec().getValueLong("id");
        long id5_02 = db.loadSql("select max(id) id from DictList where id > 0 and id < 1000").getCurRec().getValueLong("id");
        if (id5_02 > id5_01) {
            id5 = id5_01 + rnd.nextInt((int) (id5_02 - id5_01));
            dbu.updateRec("DictList", UtCnv.toMap(
                    "id", id5,
                    "Name", "-upd-ws:" + ws_id + "-" + rnd.nextInt()
            ), "Name");
        }


        // ---
        long id1 = dbu.getNextGenerator("g_ulz");
        dbu.insertRec("ulz", UtCnv.toMap(
                "id", id1,
                "region", id0,
                "ulzTip", 2,
                "name", "Name-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ));


        // ---
        long id2 = dbu.getNextGenerator("g_lic");
        dbu.insertRec("lic", UtCnv.toMap(
                "lic", id2,
                "ulz", id1,
                "NameF", "Name-F-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "Name-I-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");


        //
        if (rnd.nextInt(5) == 0) {
            long id_del = db.loadSql("select min(Lic.id) id from Lic left join pawnchit on (Lic.id = PawnChit.Lic) where Lic.id <> 0 and PawnChit.id is null").getCurRec().getValueLong("id");
            if (id_del > 0) {
                dbu.deleteRec("lic", id_del);
            }
        }
        if (rnd.nextInt(5) == 0) {
            long id_del = db.loadSql("select max(Lic.id) id from Lic left join pawnchit on (Lic.id = PawnChit.Lic) where Lic.id <> 0 and PawnChit.id is null").getCurRec().getValueLong("id");
            if (id_del > 0) {
                dbu.deleteRec("lic", id_del);
            }
        }

        //
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        long id02 = db.loadSql("select max(id) id from lic where id > " + (id01 + 100)).getCurRec().getValueLong("id");

        if (id01 > 0) {
            dbu.updateRec("lic", UtCnv.toMap(
                    "id", id01,
                    "NameF", "Name-F-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                    "NameI", "Name-I-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                    "NameO", "Name-O-upd-ws:" + ws_id + "-" + rnd.nextInt()
            ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info");
        }

        if (id02 > 0) {
            dbu.updateRec("lic", UtCnv.toMap(
                    "id", id02,
                    "NameF", "NameF-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                    "NameI", "NameI-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                    "NameO", "NameO-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                    "Ulz", id1
            ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");
        }

        // ---
        // Апдейт общей записи Lic
        dbu.db.execSql("update Lic set NameI = :NameI where Dom = :Dom", UtCnv.toMap(
                "Dom", "12",
                "NameI", "NameI-upd-com-ws:" + ws_id + "-" + rnd.nextInt()
        ));


        // ---
        // Апдейт таблиц TEST_TABLE_**
        for (IJdxTable table : struct.getTables()) {
            if (table.getName().startsWith("TEST_TABLE_")) {
                int cnt = db.loadSql("select count(*) cnt from " + table.getName() + " where id > 0").getCurRec().getValueInt("cnt");
                long id = db.loadSql("select min(id) id from " + table.getName() + " where id > 0").getCurRec().getValueLong("id");
                cnt = rnd.nextInt(cnt * 2);
                for (int x = 0; x < cnt; x++) {
                    id = db.loadSql("select min(id) id from " + table.getName() + " where id > " + id).getCurRec().getValueLong("id");
                }
                if (id > 0) {
                    // Поле name
                    dbu.db.execSql("update " + table.getName() + " set name = :name where id = :id", UtCnv.toMap(
                            "id", id,
                            "name", "upd-ws:" + ws_id + "-" + rnd.nextInt()
                    ));
                    // Поля TEST_FIELD_***
                    for (IJdxField field : table.getFields()) {
                        String fieldName = field.getName();
                        if (fieldName.startsWith("TEST_FIELD_")) {
                            dbu.db.execSql("update " + table.getName() + " set " + fieldName + " = :" + fieldName + " where id = :id", UtCnv.toMap(
                                    "id", id,
                                    fieldName, "upd-ws:" + ws_id + "-" + rnd.nextInt()
                            ));
                        }
                    }

                }
            }
        }

    }

    String getFirstRandomTable() throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        //
        for (IJdxTable t : struct.getTables()) {
            if (t.getName().startsWith("TEST_TABLE_")) {
                return t.getName();
            }
        }
        //
        return null;
    }

    int getLastRandomFieldNo(IJdxTable table) {
        String lastFieldName = null;
        //
        for (IJdxField field : table.getFields()) {
            if (field.getName().startsWith("TEST_FIELD_")) {
                lastFieldName = field.getName();
            }
        }
        //
        if (lastFieldName == null) {
            return 0;
        } else {
            return Integer.valueOf(lastFieldName.split("_")[2]);
        }
    }

    private long getDbSeed() throws Exception {
        String sql = "  select\n" +
                "  sum(region.id)+sum(regionTip.id)+sum(Ulz.id)+sum(UlzTip.id)+\n" +
                "  count(region.id)+count(regionTip.id)+count(Ulz.id)+count(UlzTip.id) as n\n" +
                "  from region\n" +
                "  join regionTip on (regionTip.id = region.regionTip)\n" +
                "  join Ulz on (Ulz.region = region.id)\n" +
                "  join UlzTip on (UlzTip.id = Ulz.UlzTip)";
        return db.loadSql(sql).getCurRec().getValueLong("n");
    }


    /**
     * Цикл вставки и удаления влияющей записи:
     * Вставка A1
     * Фиксация возраста
     * Вставка B1 со ссылкой на тольтко что вставленную А1
     * Фиксация возраста
     * Обновление B1 - замена ссылки с только что вставленной на уже существующую А0
     * Фиксация возраста
     * Удаление только что вставленной A1
     */
    void make_InsDel(IJdxDbStruct struct, long ws_id) throws Exception {
        DbUtils dbu = new DbUtils(db, struct);
        //UtRepl utRepl = new UtRepl(db, struct);
        Random rnd = new Random();
        rnd.setSeed(getDbSeed());

        // Постоянная id для regionTip
        long id1_regionTip = db.loadSql("select min(id) id from regionTip where id > 0").getCurRec().getValueLong("id");
        //long age;


        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);

        // Вставка A1 (regionTip)
        long id0_regionTip = dbu.getNextGenerator("g_regionTip");
        dbu.insertRec("regionTip", UtCnv.toMap(
                "id", id0_regionTip,
                "deleted", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);

        // Вставка B1 (region) со ссылкой на тольтко что вставленную А1 (regionTip)
        long id1_region = dbu.getNextGenerator("g_region");
        dbu.insertRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id0_regionTip,
                "parent", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);

        // Обновление B1 (region) - замена ссылки на А1 (regionTip) с только что вставленнуй на уже существующую А0 (regionTip)
        dbu.updateRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id1_regionTip,
                "parent", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);

        // Удаление только что вставленной A1 (regionTip)
        dbu.deleteRec("regionTip", id0_regionTip);

        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);
    }

    /**
     * Цикл вставки и удаления влияющей записи:
     * <p>
     * Вставка B1 со ссылкой на существующую А0
     * Фиксация возраста
     * Вставка A1
     * Фиксация возраста
     * Обновление B1 - замена ссылки с A0 на только что вставленную А1
     * Фиксация возраста
     */
    void make_InsDel_1(IJdxDbStruct struct, long ws_id) throws Exception {
        DbUtils dbu = new DbUtils(db, struct);
        //UtRepl utRepl = new UtRepl(db, struct);
        Random rnd = new Random();
        rnd.setSeed(getDbSeed());

        // Фиксация возраста
        //long age;
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);

        // Постоянная A0 (id для regionTip)
        long id0_regionTip = db.loadSql("select min(id) id from regionTip where id > 0").getCurRec().getValueLong("id");


        // Вставка B1 (region) со ссылкой на существующую вставленную А0 (regionTip)
        long id1_region = dbu.getNextGenerator("g_region");
        dbu.insertRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id0_regionTip,
                "parent", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);


        // Вставка A1 (regionTip)
        long id1_regionTip = dbu.getNextGenerator("g_regionTip");
        dbu.insertRec("regionTip", UtCnv.toMap(
                "id", id1_regionTip,
                "deleted", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);


        // Обновление B1 (region) - замена ссылки с существующей А0 (regionTip) на только что вставленную А1
        dbu.updateRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id1_regionTip,
                "parent", 0,
                "name", "name-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);
    }

}
