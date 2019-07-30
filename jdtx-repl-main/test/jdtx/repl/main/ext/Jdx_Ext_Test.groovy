package jdtx.repl.main.ext


import jandcode.jc.ProjectScript
import jandcode.jc.test.JcTestCase
import jandcode.utils.variant.IVariantMap
import jandcode.utils.variant.VariantMap
import org.junit.Test

class Jdx_Ext_Test extends JcTestCase {

    Jdx_Ext ext;

    String cfg_json_ws = "test/etalon/ws.json";


    @Override
    void setUp() throws Exception {
        super.setUp()
        ProjectScript p = jc.loadProject("srv/project.jc")
        ext = p.createExt("jdtx.repl.main.ext.Jdx_Ext")
    }

    @Test
    public void test_repl_info() {
        IVariantMap args = new VariantMap();

        ext.repl_info(args)
    }

    @Test
    public void xxx() {
        IVariantMap args = new VariantMap();
        args.put("ws", 1)
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401")
        args.put("cfg", cfg_json_ws)

        ext.repl_create(args)
    }
}
