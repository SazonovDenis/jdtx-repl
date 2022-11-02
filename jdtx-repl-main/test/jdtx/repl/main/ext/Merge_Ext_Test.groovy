package jdtx.repl.main.ext

import jandcode.dbm.*
import jandcode.dbm.db.*
import jandcode.jc.*
import jandcode.jc.test.*
import jandcode.utils.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.struct.*
import org.junit.*

class Merge_Ext_Test extends JcTestCase {


    Merge_Ext ext
    Merge_Ext ext1
    Merge_Ext ext2

    @Override
    void setUp() throws Exception {
        super.setUp()

        ProjectScript project = jc.loadProject("one/project.jc")
        ext = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext")

        ProjectScript project1 = jc.loadProject("srv/project.jc")
        ext1 = (Merge_Ext) project1.createExt("jdtx.repl.main.ext.Merge_Ext")

        ProjectScript project2 = jc.loadProject("ws2/project.jc")
        ext2 = (Merge_Ext) project2.createExt("jdtx.repl.main.ext.Merge_Ext")
    }

    @Test
    void repl_record_merge_file() {
        IVariantMap args = new VariantMap()
        //args.put("table", "LicDocTip")
        //args.put("file", "temp/_LicDocTip.task")
        //args.put("fields", "Name,ShortName")
        args.put("table", "LicDocTip")
        args.put("file", "temp/_LicDocTip.task")
        args.put("fields", "Name")
        args.put("cfg_group", "test/etalon/field_groups.json")

        //
        ext2.rec_merge_find(args)

        //
        ext2.rec_merge_exec(args)

        //
        ext2.rec_merge_find(args)
    }

    @Test
    void rec_merge_revert() {
        IVariantMap args = new VariantMap()
        args.put("tables", "LicDocTip,LicDocVid")
        args.put("file", "temp/229990_result.zip")

        //
        ext2.rec_merge_revert(args)
    }

    @Test
    void relocate() {
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        String tempDirName = "temp/relocate/";
        UtFile.cleanDir(tempDirName);

        //
        IVariantMap args = new VariantMap()
        args.put("table", "Lic")
        args.put("sour", 1357)
        args.put("outFile", tempDirName + "relocate_check_Lic_1357.result.zip")

        //
        System.out.println("rec_relocate_check")
        ext.rec_relocate_check(args)

        //
        System.out.println("rec_relocate")
        args.put("outDir", tempDirName + "temp1")
        args.put("sour", "1357,1358")
        args.put("dest", "7777,7778")
        ext.rec_relocate(args)

        //
        System.out.println("rec_relocate back")
        args.put("outDir", tempDirName + "temp2")
        args.put("sour", "7777,7778")
        args.put("dest", "1357,1358")
        ext.rec_relocate(args)
    }

    @Test
    void relocate_range() {
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        String tempDirName = "temp/relocate/";
        UtFile.cleanDir(tempDirName);

        //
        IVariantMap args = new VariantMap()
        args.put("table", "Lic")

        //
        System.out.println("rec_relocate range")
        args.put("outDir", tempDirName + "temp1")
        args.put("sourFrom", "1000")
        args.put("sourTo", "10000")
        args.put("destFrom", "20000")
        ext.rec_relocate(args)

        //
        System.out.println("rec_relocate range back")
        args.put("outDir", tempDirName + "temp2")
        args.put("sourFrom", "20000")
        args.put("sourTo", "99999")
        args.put("destFrom", "1000")
        ext.rec_relocate(args)
    }

    @Test
    void relocate_TBD_AGENT_TYPE() {
        Db db = ext2.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        println("db: " + db.getDbSource().getHost() + ":" + db.getDbSource().getDatabase() + "@" + db.getDbSource().getUsername())

        // ov rec-relocate -table:AGENT_TYPE -sour:2 -dest:200
        IVariantMap args = new VariantMap()
        args.put("table", "AGENT_TYPE")
        args.put("sour", 2)
        args.put("dest", 200)

        //
        System.out.println("rec_relocate")
        args.put("outDir", "temp")
        ext2.rec_relocate(args)
    }

    @Test
    void relocate_TBD_WELL() {
        Db db = ext2.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        println("db: " + db.getDbSource().getHost() + ":" + db.getDbSource().getDatabase() + "@" + db.getDbSource().getUsername())

        // ov rec-relocate -outDir:Z:\jdtx-repl\temp -table:WELL -sour:1000,1001 -dest:3000000000000,3000000000001
        IVariantMap args = new VariantMap()
        args.put("table", "WELL")
        args.put("outDir", "temp")
        args.put("dest", "1000,1001")
        args.put("sour", "3000000000000,3000000000001")

        //
        System.out.println("rec_relocate")
        ext2.rec_relocate(args)
    }

    @Test
    void relocate_file_TBD_AGENT_TYPE() {
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        println("db: " + db.getDbSource().getHost() + ":" + db.getDbSource().getDatabase() + "@" + db.getDbSource().getUsername())

        // ov rec-relocate -outDir:d:\t -table:agent_type -file:D:\t\ids_agent_type.csv
        IVariantMap args = new VariantMap()
        args.put("table", "agent_type")
        args.put("outDir", "d:/t")
        args.put("file", "D:/t/ids_agent_type.csv")

        //
        System.out.println("rec_relocate - file")
        ext.rec_relocate(args)
    }

    @Test
    void relocate_file_TBD_WELL() {
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        println("db: " + db.getDbSource().getHost() + ":" + db.getDbSource().getDatabase() + "@" + db.getDbSource().getUsername())

        // ov rec-relocate -outDir:d:\t -table:well -file:D:\t\ids_well.csv
        IVariantMap args = new VariantMap()
        args.put("table", "well")
        args.put("outDir", "d:/t")
        args.put("file", "D:/t/ids_well.csv")

        //
        System.out.println("rec_relocate - file")
        ext.rec_relocate(args)
    }

    @Test
    void readDbStruct_TBD() throws Exception {
        Db db = ext2.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        println("db: " + db.getDbSource().getHost() + ":" + db.getDbSource().getDatabase() + "@" + db.getDbSource().getUsername())


        //
        JdxDbStructReader dbStructReader = new JdxDbStructReader()
        dbStructReader.setDb(db)
        IJdxDbStruct struct = dbStructReader.readDbStruct()


        //
        JdxDbStruct_XmlRW xmlRW = new JdxDbStruct_XmlRW();
        xmlRW.toFile(struct, "temp/dbStruct.xml");
    }

}
