package jdtx.repl.main

import jandcode.app.test.AppTestCase
import jandcode.bgtasks.BgTasksService
import jandcode.dbm.ModelService
import jandcode.dbm.db.Db
import jandcode.utils.UtClass
import jandcode.utils.error.XError
import jandcode.utils.variant.IVariantMap
import jandcode.utils.variant.VariantMap
import jdtx.repl.main.api.IJdxMailer
import jdtx.repl.main.api.JdxReplWs
import jdtx.repl.main.api.UtMailerLocalFiles
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.junit.Test

/**
 */
class Jdx_Ext_Test extends AppTestCase {

    protected static Log log = LogFactory.getLog("jdtx");

    @Test
    public void test_1() throws Exception {
        IVariantMap args = new VariantMap()
        args.put("dir", "../_test-data/mail_local/")
        args.put("age_from", 1)
        args.put("age_to", 2)

        Jdx_Ext ext = (Jdx_Ext) UtClass.createInst(Jdx_Ext.class);
        ext.repl_send(args)  // todo: не запускается - неправильно инициализован
    }




}
