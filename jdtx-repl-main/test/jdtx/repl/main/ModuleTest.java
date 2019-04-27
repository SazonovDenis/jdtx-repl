package jdtx.repl.main;

import jandcode.app.test.*;
import jandcode.bgtasks.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import org.junit.*;

public class ModuleTest extends AppTestCase {

    {
        logSetUp = false;
    }

    @Test
    public void test1() throws Exception {
        app.saveAppRt();
    }

    @Test
    public void test_bg() throws Exception {
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName");
        System.out.println("cfgFileName: " + cfgFileName);
    }

    @Test
    public void test_ver() throws Exception {
        VersionInfo vi;
        vi = new VersionInfo("jdtx.repl.main.api");
        System.out.println(vi.getVersion());
        vi = new VersionInfo("jdtx.repl.main");
        System.out.println(vi.getVersion());
        vi = new VersionInfo("jdtx.repl");
        System.out.println(vi.getVersion());
        vi = new VersionInfo("jdtx");
        System.out.println(vi.getVersion());
        //
        System.out.println(UtRepl.getVersion());
    }

}
