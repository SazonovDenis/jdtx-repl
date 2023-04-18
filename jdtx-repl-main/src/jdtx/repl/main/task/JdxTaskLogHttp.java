package jdtx.repl.main.task;

import com.jdtx.state.*;
import com.jdtx.tree.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.log.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Перенос содержимого лога на почтовый сервер
 */
public class JdxTaskLogHttp extends JdxTaskCustom {

    //
    IMailer mailer;

    //
    public JdxTaskLogHttp(IMailer mailer) {
        this.mailer = mailer;
        log = LogFactory.getLog("jdtx.JdxTaskLogHttp");
    }

    //
    public void doTask() throws Exception {
        // Соберем из JdtxLogContainer
        Map<String, String> logValues = JdtxLogContainer.getLogValues();
        Map<String, Object> data = new HashMap<>();
        data.put("logValues", logValues);
        // Отправим
        try {
            mailer.setData(data, "log.log", null);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }

        // Соберем из JdtxStateContainer.state
        ITreeNode<StateItem> root = JdtxStateContainer.state.getAll();
        Map<String, Object> stateValue = UtStateCnv.stateItemToMap(root);
        // Отправим
        try {
            mailer.setData(stateValue, "state.json", null);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

}
