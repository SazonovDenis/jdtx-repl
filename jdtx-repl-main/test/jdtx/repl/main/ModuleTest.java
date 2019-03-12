package jdtx.repl.main;

import jandcode.app.test.AppTestCase;
import jandcode.bgtasks.BgTasksService;
import org.junit.Test;

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


}
