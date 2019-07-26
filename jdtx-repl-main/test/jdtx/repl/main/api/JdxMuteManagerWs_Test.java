package jdtx.repl.main.api;

import org.junit.*;

public class JdxMuteManagerWs_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_0() throws Exception {
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);

        //
        System.out.println("mute = " + utmm.isMute());
        utmm.muteWorkstation();
        System.out.println("mute = " + utmm.isMute());
        utmm.unmuteWorkstation();
        System.out.println("mute = " + utmm.isMute());
    }

    @Test
    public void test_1() throws Exception {
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db2);
        //
        UtTest utTest = new UtTest(db2);
        //
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        System.out.println("--------------------------------------------");
        System.out.println("mute = " + utmm.isMute());

        // Делаем изменения
        utTest.makeChange(struct2, 2);

        // Отслеживаем и обрабатываем свои изменения
        System.out.println("--------------------------------------------");
        System.out.println("Отслеживаем и обрабатываем свои изменения");
        System.out.println("mute = " + utmm.isMute());
        ws.handleSelfAudit();
        System.out.println("--------------------------------------------");
        ws.handleSelfAudit();


        //
        System.out.println("--------------------------------------------");
        System.out.println("muteWorkstation");
        utmm.muteWorkstation();


        // Делаем изменения
        utTest.makeChange(struct2, 2);

        // Отслеживаем и обрабатываем свои изменения
        System.out.println("--------------------------------------------");
        System.out.println("Отслеживаем и обрабатываем свои изменения");
        System.out.println("mute = " + utmm.isMute());
        ws.handleSelfAudit();
        System.out.println("--------------------------------------------");
        ws.handleSelfAudit();


        //
        System.out.println("--------------------------------------------");
        System.out.println("unmuteWorkstation");
        utmm.unmuteWorkstation();

        // Отслеживаем и обрабатываем свои изменения
        System.out.println("--------------------------------------------");
        System.out.println("Отслеживаем и обрабатываем свои изменения");
        System.out.println("mute = " + utmm.isMute());
        ws.handleSelfAudit();
        System.out.println("--------------------------------------------");
        ws.handleSelfAudit();
    }


}
