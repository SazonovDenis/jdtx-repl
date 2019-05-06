package jdtx.repl.main.ext;

import jandcode.app.*;
import jandcode.jc.*;
import jandcode.utils.*;
import jandcode.web.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtSetup extends ProjectExt {

    App app;

    public void gen(String inFileName, String outDirName) throws Exception {
        //System.out.println(inFileName + " -> " + outDirName);

        //
        RandomString rnd = new RandomString();
        Map args_srv = new HashMap();

        // Читаем описание
        String[] lines = UtFile.loadString(inFileName, "windows-1251").split("\n");

        // Главный guid
        String name_main = lines[0].trim();
        args_srv.put("guid_repl", rnd.nextHexStr(16) + "." + name_main);


        // ---
        // Формируем список параметров...
        List ws_list = new ArrayList<>();

        // ... сервер,
        Map ws = new HashMap<>();
        ws.put("guid", "01" + rnd.nextHexStr(14));
        ws.put("name", "Server");
        ws_list.add(ws);

        // ... станции
        for (int no = 1; no < lines.length; no++) {
            String line = lines[no].trim();
            ws = new HashMap<>();
            ws.put("guid", UtString.padLeft(String.valueOf(no + 1), 2, "0") + rnd.nextHexStr(14));
            ws.put("name", line);
            ws_list.add(ws);
        }

        //
        args_srv.put("ws_list", ws_list);


        // Настройка сервера
        // setup.srv.bat
        OutBuilder b = new OutBuilder(app);
        b.outTml("res:jdtx/repl/main/ext/setup.srv.bat.gsp", args_srv, null);
        UtFile.saveString(b.toString(), new File(outDirName + "setup." + name_main + ".srv.bat"), "cp866");


        // Настройка рабочих станций
        // setup.ws.bat
        for (int no = 1; no < lines.length; no++) {
            Map args_ws = new HashMap();
            ws = (Map) ws_list.get(no);
            args_ws.put("guid_repl", args_srv.get("guid_repl"));
            args_ws.put("guid", ws.get("guid"));
            args_ws.put("no", no);
            // Файл
            b = new OutBuilder(app);
            b.outTml("res:jdtx/repl/main/ext/setup.ws.bat.gsp", args_ws, null);
            UtFile.saveString(b.toString(), new File(outDirName + "setup." + name_main + ".ws" + no + ".bat"), "cp866");
        }
    }
}
