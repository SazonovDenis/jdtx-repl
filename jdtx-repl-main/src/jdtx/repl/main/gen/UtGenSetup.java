package jdtx.repl.main.gen;

import jandcode.app.*;
import jandcode.jc.*;
import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.ut.*;

import java.io.*;
import java.util.*;

/**
 * Генератор bat-файлов для первичной настройки рабочих станций на клиенте и сервере после инсталяции.
 */
public class UtGenSetup extends ProjectExt {

    public App app;

    public void gen(String inFileName, String outDirName) throws Exception {
        RandomString rnd = new RandomString();
        Map args_srv = new HashMap();

        // Читаем описание
        String[] lines = UtFile.loadString(inFileName, "windows-1251").split("\n");

        // Главный guid
        String name_main = lines[0].trim();
        args_srv.put("repl_guid", rnd.nextHexStr(16) + "." + name_main);


        // ---
        // Формируем список рабочих станций...
        List ws_list = new ArrayList<>();

        // ... сервер (ws_no = 1),
        Map ws = new HashMap<>();
        ws.put("ws_guid", "01" + rnd.nextHexStr(14));
        ws.put("ws_name", "Server");
        ws.put("ws_no", 1);
        ws_list.add(ws);

        // ... станции (ws_no = 2, 3, ...)
        for (int i = 1; i < lines.length; i++) {
            int ws_no = i + 1;
            String line = lines[i].trim();
            ws = new HashMap<>();
            ws.put("ws_guid", UtString.padLeft(String.valueOf(ws_no), 2, "0") + rnd.nextHexStr(14));
            ws.put("ws_name", line);
            ws.put("ws_no", ws_no);
            ws_list.add(ws);
        }

        //
        args_srv.put("ws_list", ws_list);


        // Файл: setup.srv.bat
        // Настройка сервера
        OutBuilder b = new OutBuilder(app);
        b.outTml("res:jdtx/repl/main/gen/UtGenSetup.srv.bat.gsp", args_srv, null);
        UtFile.saveString(b.toString(), new File(outDirName + "setup." + name_main + ".srv.bat"), "cp866");


        // Файл: setup.ws.bat
        // Настройка рабочих станций
        for (int i = 1; i < lines.length; i++) {
            ws = (Map) ws_list.get(i);
            Map args_ws = new HashMap();
            args_ws.put("repl_guid", args_srv.get("repl_guid"));
            args_ws.put("ws_guid", ws.get("ws_guid"));
            args_ws.put("ws_name", ws.get("ws_name"));
            args_ws.put("ws_no", ws.get("ws_no"));
            // Файл
            b = new OutBuilder(app);
            b.outTml("res:jdtx/repl/main/gen/UtGenSetup.ws.bat.gsp", args_ws, null);
            UtFile.saveString(b.toString(), new File(outDirName + "setup." + name_main + ".ws" + ws.get("ws_no") + ".bat"), "cp866");
        }

        // Файл: ws_list.json
        // Список рабочих станций
        b = new OutBuilder(app);
        b.outTml("res:jdtx/repl/main/gen/UtGenSetup.ws_list.json.gsp", args_srv, null);
        UtFile.saveString(b.toString(), new File(outDirName + "ws_list.json"), "utf-8");
    }
}
