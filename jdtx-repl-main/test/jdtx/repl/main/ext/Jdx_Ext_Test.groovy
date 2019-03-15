package jdtx.repl.main.ext

import jandcode.app.test.AppTestCase
import jandcode.utils.UtClass
import jandcode.utils.variant.IVariantMap
import jandcode.utils.variant.VariantMap
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
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
