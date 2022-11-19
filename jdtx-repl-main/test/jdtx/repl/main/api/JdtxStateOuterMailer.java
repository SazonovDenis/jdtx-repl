package jdtx.repl.main.api;

import com.jdtx.state.StateItem;
import com.jdtx.state.StateItemStack;
import com.jdtx.state.UtStateCnv;
import com.jdtx.state.impl.StateItemStackThread;
import com.jdtx.tree.ITreeNode;
import jdtx.repl.main.api.mailer.IMailer;
import jdtx.repl.main.log.JdtxStateContainer;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.Map;

public class JdtxStateOuterMailer implements Runnable {


    private IMailer mailer;
    private StateItemStack stateItemStack;
    private long sleepDuration = 100;


    public JdtxStateOuterMailer(StateItemStackThread stateItemStack, IMailer mailer) {
        this.stateItemStack = stateItemStack;
        this.mailer = mailer;
    }


    @Override
    public void run() {
        System.out.println("Started: " + this.getClass().getName() + ", mailer: " + mailer.getClass().getName());

        //
        while (true) {
            try {
                ITreeNode<StateItem> root = stateItemStack.getAll();

                //
                Map<String, Object> stateValue = UtStateCnv.stateItemToMap(root);
                try {
                    mailer.setData(stateValue, "state.json", null);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

                //
                try {
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
