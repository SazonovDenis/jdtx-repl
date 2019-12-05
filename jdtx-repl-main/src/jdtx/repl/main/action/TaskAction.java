package jdtx.repl.main.action;

import jandcode.bgtasks.BgTask;
import jandcode.bgtasks.BgTasksInfo;
import jandcode.bgtasks.BgTasksService;
import jandcode.utils.UtCnv;
import jandcode.web.UtJson;
import jandcode.web.WebAction;
import jdtx.repl.main.task.WsBgTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Управление репликатором через web
 */
public class TaskAction extends WebAction {


    protected static Log log = LogFactory.getLog("jdtx.TaskAction");


    /**
     * Запуск задачи вне расписания по команде
     */
    public void start() throws IOException {
        //
        log.info("Immediate start WsBgTask");

        //
        BgTasksInfo ti = getApp().service(BgTasksService.class).getTasksInfo();

        //
        for (BgTask task : ti.getQue()) {
            if (task.getName().equals("ws")) {
                ((WsBgTask) task).runImmediate = true;
            }
        }

        //
        for (BgTask task : ti.getRunned()) {
            if (task.getName().equals("ws")) {
                ((WsBgTask) task).runImmediate = true;
            }
        }

        //
        String res = UtJson.toString(UtCnv.toMap(
                "result", "ok"
        ));
        getRequest().getHttpResponse().setCharacterEncoding("UTF-8");
        getRequest().getOutWriter().write(res);
    }


    public void info() throws IOException {
        Map info = null;
        BgTasksInfo ti = getApp().service(BgTasksService.class).getTasksInfo();
        for (BgTask task : ti.getRunned()) {
            if (task.getName().equals("ws")) {
                info = task.getLogger().getData();
            }
        }

        //
        String res = UtJson.toString(UtCnv.toMap("result", info));
        getRequest().getHttpResponse().setCharacterEncoding("UTF-8");
        getRequest().getOutWriter().write(res);
    }


}
