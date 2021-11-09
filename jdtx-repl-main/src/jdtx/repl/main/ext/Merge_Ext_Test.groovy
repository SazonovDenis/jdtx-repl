package jdtx.repl.main.ext

import jandcode.dbm.*
import jandcode.dbm.db.*
import jandcode.jc.*
import jandcode.jc.test.*
import jandcode.utils.variant.*
import org.junit.*


class Merge_Ext_Test extends JcTestCase {


    Merge_Ext ext

    @Override
    void setUp() throws Exception {
        super.setUp()
        ProjectScript project = jc.loadProject("ws2/project.jc")
        ext = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext")
    }

    @Test
    void repl_record_merge_file() {
        IVariantMap args = new VariantMap();
        //args.put("table", "LicDocTip");
        //args.put("file", "temp/_LicDocTip.task");
        //args.put("fields", "Name,ShortName");
        args.put("table", "LicDocTip");
        args.put("file", "temp/_LicDocTip.task")
        args.put("fields", "Name")
        args.put("cfg_group", "test/etalon/field_groups.json")

        //
        ext.rec_merge_find(args);

        //
        ext.rec_merge_exec(args)

        //
        ext.rec_merge_find(args);
    }

    @Test
    void relocate() {
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        //UtData.outTable(db.loadSql("select * from Lic order by Id"));

        //
        IVariantMap args = new VariantMap();
        args.put("table", "Lic");
        args.put("sour", 1357)
        args.put("dest", 7777)

        //
        System.out.println("==================")
        System.out.println("rec_relocate_check")
        ext.rec_relocate_check(args);

        //
        System.out.println("==================")
        System.out.println("rec_relocate")
        ext.rec_relocate(args);
    }


}
