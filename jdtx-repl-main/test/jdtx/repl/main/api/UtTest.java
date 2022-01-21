package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 *
 */
public class UtTest extends UtilsTestCase {

    Db db;

    public UtTest(Db db) {
        this.db = db;
    }

    static class JdxRandom extends Random {
        public String nextStr(int len) {

            String res = new DateTime().toString("HHmmss.SSS");
            res = res + "-" + Math.abs(nextInt());
            res = UtString.padRight(res, len, "-").substring(0, len);

            return res;
        }
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
            if (!UtDbErrors.errorIs_TableNotExists(e)) {
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
        JdxRandom rnd = new JdxRandom();
        rnd.setSeed(getDbSeed());
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        //
        long id = dbu.getNextGenerator("g_" + tableName);
        sql = "insert into " + tableName + " (id, name) values (" + id + ", '" + "new~" + rnd.nextStr(14) + "')";
        db.execSql(sql);
        //
        id = dbu.getNextGenerator("g_" + tableName);
        sql = "insert into " + tableName + " (id, name) values (" + id + ", '" + "new~" + rnd.nextStr(14) + "')";
        db.execSql(sql);
    }

    public void makeChangeUnimportant(IJdxDbStruct struct, long ws_id) throws Exception {
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        DataRecord rec = db.loadSql("select * from lic where id = " + id01).getCurRec();
        dbu.updateRec("lic",
                UtCnv.toMap("id", id01, "NameF", rec.getValue("NameF")),
                "NameF",
                "NameI,NameO,bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info"
        );
    }

    public void makeChange_CommentTip(IJdxDbStruct struct, long ws_id) throws Exception {
        JdxRandom rnd = new JdxRandom();
        rnd.setSeed(getDbSeed());
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        // --- commentTip
        long id_commentTip = dbu.getNextGenerator("g_commentTip");
        Map params = UtCnv.toMap(
                "Id", id_commentTip,
                "Deleted", 0,
                "Name", "Tip-ins-ws:" + ws_id + "-" + rnd.nextStr(14)
        );
        //
        dbu.insertRec("commentTip", params);

        // --- commentText
        long id_commentText = dbu.getNextGenerator("g_commentText");
        params = UtCnv.toMap(
                "id", id_commentText,
                "CommentTip", id_commentTip,
                "CommentDt", new DateTime(),
                "CommentUsr", dbu.loadSqlRec("select max(id) id from Usr", null).getValueLong("id"),
                "PawnChit", 0,
                "PawnChitSubject", 0,
                "Lic", dbu.loadSqlRec("select max(id) id from Lic", null).getValueLong("id"),
                "CommentText", "LicInsWs:" + ws_id + "-" + rnd.nextStr(14)
        );
        dbu.insertRec("commentText", params);
    }

    public void makeChange(IJdxDbStruct struct, long ws_id) throws Exception {
        JdxRandom rnd = new JdxRandom();
        rnd.setSeed(getDbSeed());
        JdxDbUtils dbu = new JdxDbUtils(db, struct);


        // --- region
        long id0 = dbu.getNextGenerator("g_region");
        Map params = UtCnv.toMap(
                "id", id0,
                "regionTip", 1,
                "parent", 0,
                "name", "Ins-ws:" + ws_id + "-" + rnd.nextStr(14)
        );
        //
        String regionTestFields = "";
        for (IJdxField f : struct.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields = regionTestFields + "Region." + f.getName() + ",";
                params.put(f.getName(), f.getName() + "-ins-ws:" + ws_id + "-" + rnd.nextStr(14));
            }
        }
        //
        dbu.insertRec("region", params);


        // --- UsrLog.Info
        long id5 = dbu.getNextGenerator("g_UsrLog");
        dbu.insertRec("UsrLog", UtCnv.toMap(
                "id", id5,
                "Info", "-ins-ws:" + ws_id + ":" + id5 + "-" + rnd.nextStr(10)
        ), "Info");
        id5 = dbu.getNextGenerator("g_UsrLog");
        dbu.insertRec("UsrLog", UtCnv.toMap(
                "id", id5,
                "Info", "-ins-ws:" + ws_id + ":" + id5 + "-" + rnd.nextStr(10)
        ), "Info");
        // диапазон "старых" значений
        long id5_01 = db.loadSql("select min(id) id from UsrLog where id > 0 and id < 2000").getCurRec().getValueLong("id");
        long id5_02 = db.loadSql("select max(id) id from UsrLog where id > 0 and id < 2000").getCurRec().getValueLong("id");
        long id5_Diff = (id5_02 - id5_01) / 2; // Половина диапазона
        if (id5_Diff > 0) {
            id5 = id5_01 + id5_Diff + rnd.nextInt((int) id5_Diff);
            dbu.updateRec("UsrLog", UtCnv.toMap(
                    "id", id5,
                    "Info", "-updWs:" + ws_id + ":" + id5 + "-" + rnd.nextStr(10)
            ), "Info");
        }


        // --- Ulz
        long id_Ulz = dbu.getNextGenerator("g_ulz");
        dbu.insertRec("ulz", UtCnv.toMap(
                "id", id_Ulz,
                "region", id0,
                "ulzTip", 2,
                "name", "InsWs:" + ws_id + "-" + rnd.nextStr(14)
        ));


        // --- Lic
        long id_Lic = dbu.getNextGenerator("g_lic");
        dbu.insertRec("lic", UtCnv.toMap(
                "id", id_Lic,
                "ulz", id_Ulz,
                "NameF", "InsWs:" + ws_id + "-" + rnd.nextStr(14),
                "NameI", "InsWs:" + ws_id + "-" + rnd.nextStr(14),
                "NameO", "InsWs:" + ws_id + "-" + rnd.nextStr(14)
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");


        //
        if (rnd.nextInt(5) == 0) {
            long id_Lic_del = db.loadSql("select min(Lic.id) id from Lic left join pawnchit on (Lic.id = PawnChit.Lic) where Lic.id <> 0 and PawnChit.id is null").getCurRec().getValueLong("id");
            if (id_Lic_del > 0) {
                dbu.deleteRec("lic", id_Lic_del);
            }
        }
        if (rnd.nextInt(5) == 0) {
            long id_Lic_del = db.loadSql("select max(Lic.id) id from Lic left join pawnchit on (Lic.id = PawnChit.Lic) where Lic.id <> 0 and PawnChit.id is null").getCurRec().getValueLong("id");
            if (id_Lic_del > 0) {
                dbu.deleteRec("lic", id_Lic_del);
            }
        }


        //
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        long id02 = db.loadSql("select max(id) id from lic where id > " + (id01 + 100)).getCurRec().getValueLong("id");

        if (id01 > 0) {
            dbu.updateRec("lic", UtCnv.toMap(
                    "id", id01,
                    "NameF", "UpdWs:" + ws_id + "-" + rnd.nextStr(14),
                    "NameI", "UpdWs:" + ws_id + "-" + rnd.nextStr(14),
                    "NameO", "UpdWs:" + ws_id + "-" + rnd.nextStr(14)
            ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info");
        }

        if (id02 > 0) {
            dbu.updateRec("lic", UtCnv.toMap(
                    "id", id02,
                    "NameF", "UpdWs:" + ws_id + "-" + rnd.nextStr(14),
                    "NameI", "UpdWs:" + ws_id + "-" + rnd.nextStr(14),
                    "NameO", "UpdWs:" + ws_id + "-" + rnd.nextStr(14),
                    "Ulz", id_Ulz
            ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");
        }

        // ---
        // Апдейт общей записи Lic
        db.execSql("update Lic set NameI = :NameI where Dom = :Dom", UtCnv.toMap(
                "Dom", "12",
                "NameI", "UpCWs:" + ws_id + "-" + rnd.nextStr(14)
        ));


        // --- CommentText
        //long id_CommentText = dbu.getNextGenerator("g_CommentText");
        long id_Usr = db.loadSql("select max(Usr.id) id from Usr where Usr.id <> 0").getCurRec().getValueLong("id");
        long id_Lic_CommentText = db.loadSql("select max(Lic.id) id from Lic left join pawnchit on (Lic.id = PawnChit.Lic) where Lic.id <> 0 and PawnChit.id is null").getCurRec().getValueLong("id");
        long id_PawnChit = db.loadSql("select max(PawnChit.id) id from PawnChit where PawnChit.id <> 0").getCurRec().getValueLong("id");
        dbu.insertRec("CommentText", UtCnv.toMap(
                "Lic", id_Lic_CommentText,
                "PawnChit", 0,
                "CommentTip", 1 + rnd.nextInt(3),
                "CommentUsr", id_Usr,
                "CommentDt", new DateTime(),
                "CommentText", "LicInsWs:" + ws_id + "-" + rnd.nextStr(14)
        ), "lic,pawnChit,CommentTip,CommentUsr,CommentDt,CommentText", null);
        if (id_PawnChit != 0) {
            dbu.insertRec("CommentText", UtCnv.toMap(
                    "Lic", 0,
                    "PawnChit", id_PawnChit,
                    "CommentTip", 1 + rnd.nextInt(3),
                    "CommentUsr", id_Usr,
                    "CommentDt", new DateTime(),
                    "CommentText", "PawInsWs:" + ws_id + "-" + rnd.nextStr(14)
            ), "lic,pawnChit,CommentTip,CommentUsr,CommentDt,CommentText", null);
        }

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
                    db.execSql("update " + table.getName() + " set name = :name where id = :id", UtCnv.toMap(
                            "id", id,
                            "name", "upd-ws:" + ws_id + "-" + rnd.nextStr(14)
                    ));
                    // Поля TEST_FIELD_***
                    for (IJdxField field : table.getFields()) {
                        String fieldName = field.getName();
                        if (fieldName.startsWith("TEST_FIELD_")) {
                            db.execSql("update " + table.getName() + " set " + fieldName + " = :" + fieldName + " where id = :id", UtCnv.toMap(
                                    "id", id,
                                    fieldName, "upd-ws:" + ws_id + "-" + rnd.nextStr(14)
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

    /**
     * Рандом, зависимый от состояния БД
     */
    long getDbSeed() throws Exception {
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
     * Вставка B1 со ссылкой на тольтко что вставленную А1 (B.А -> А1)
     * Фиксация возраста
     * Обновление B1 - замена ссылки с только что вставленной на уже существующую А0  (B.А -> А0)
     * Фиксация возраста
     * Удаление только что вставленной A1
     */
    void make_Region_InsDel_0(IJdxDbStruct struct, long ws_id) throws Exception {
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        //UtRepl utRepl = new UtRepl(db, struct);
        JdxRandom rnd = new JdxRandom();
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
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14),
                "shortName", "sn-" + rnd.nextStr(10)
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
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14)
        ));

        // Фиксация возраста
        //age = utRepl.getAuditAge();
        //System.out.println("age: " + age);

        // Обновление B1 (region) - замена ссылки на А1 (regionTip) с только что вставленнуй на уже существующую А0 (regionTip)
        dbu.updateRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id1_regionTip,
                "parent", 0,
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14)
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
    void make_Region_InsDel_1(IJdxDbStruct struct, long ws_id) throws Exception {
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        //UtRepl utRepl = new UtRepl(db, struct);
        JdxRandom rnd = new JdxRandom();
        rnd.setSeed(getDbSeed());
        //^c проверить фильтр "берем все с сервера или свое" commenttip, добавляем на филиалах каждый свой commentTip, потом добавляем commentText, а потом на сервере делаем merge
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
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14)
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);


        // Вставка A1 (regionTip)
        long id1_regionTip = dbu.getNextGenerator("g_regionTip");
        dbu.insertRec("regionTip", UtCnv.toMap(
                "id", id1_regionTip,
                "deleted", 0,
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14),
                "shortName", "sn-" + rnd.nextStr(10)
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);


        // Обновление B1 (region) - замена ссылки с существующей А0 (regionTip) на только что вставленную А1
        dbu.updateRec("region", UtCnv.toMap(
                "id", id1_region,
                "regionTip", id1_regionTip,
                "parent", 0,
                "name", "Ws:" + ws_id + "-" + rnd.nextStr(14)
        ));

        // Фиксация возраста
        //age = utRepl.markAuditAge();
        //System.out.println("age: " + age);
    }

    public static void doUnzipDir(String zipFilePath, String destDir) throws IOException {
        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();
        FileInputStream fis;
        //buffer for read and write data to file
        byte[] buffer = new byte[1024];
        fis = new FileInputStream(zipFilePath);
        ZipInputStream zis = new ZipInputStream(fis);
        ZipEntry ze = zis.getNextEntry();
        while (ze != null) {
            if (!ze.isDirectory()) {
                String fileName = ze.getName();
                File newFile = new File(destDir + File.separator + fileName);
                System.out.println("Unzipping to " + newFile.getAbsolutePath());
                //create directories for sub directories in zip
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
            }
            ze = zis.getNextEntry();
        }
        //close last ZipEntry
        zis.closeEntry();
        zis.close();
        fis.close();
    }

}
