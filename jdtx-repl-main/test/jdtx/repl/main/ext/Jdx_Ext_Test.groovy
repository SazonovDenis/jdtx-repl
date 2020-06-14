package jdtx.repl.main.ext


import jandcode.jc.ProjectScript
import jandcode.jc.test.JcTestCase
import jandcode.utils.variant.IVariantMap
import jandcode.utils.variant.VariantMap
import org.junit.Test

class Jdx_Ext_Test extends JcTestCase {

    Jdx_Ext extSrv;
    Jdx_Ext extWs2;
    Jdx_Ext extWs3;

    String cfg_json_ws = "test/etalon/ws.json";


    @Override
    void setUp() throws Exception {
        super.setUp()
        ProjectScript p1 = jc.loadProject("srv/project.jc")
        extSrv = p1.createExt("jdtx.repl.main.ext.Jdx_Ext")
        ProjectScript p2 = jc.loadProject("ws2/project.jc")
        extWs2 = p2.createExt("jdtx.repl.main.ext.Jdx_Ext")
        ProjectScript p3 = jc.loadProject("ws3/project.jc")
        extWs3 = p3.createExt("jdtx.repl.main.ext.Jdx_Ext")
    }


    @Test
    public void test_repl_info() {
        IVariantMap args = new VariantMap();

        extSrv.repl_info(args)
        System.out.println("=========================")
        extWs2.repl_info(args)
        System.out.println("=========================")
        extWs3.repl_info(args)
    }


    @Test
    public void xxx() {
        IVariantMap args = new VariantMap();
        args.put("ws", 1)
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401")
        args.put("file", cfg_json_ws)

        extSrv.repl_create(args)
    }


    @Test
    public void repl_record_merge() {
        IVariantMap args = new VariantMap();
        args.put("file", "test/jdtx/repl/main/ext/UtRecMergeTest.xml")

        extSrv.repl_record_merge(args)
    }


}
