package jdtx.repl.main.api;

import com.jdtx.state.StateItem;
import com.jdtx.state.StateItemStack;
import com.jdtx.state.UtStateCnv;
import com.jdtx.state.impl.StateItemStackThread;
import com.jdtx.tree.ITreeNode;
import jdtx.repl.main.api.mailer.IMailer;
import jdtx.repl.main.api.mailer.MailerHttp;
import jdtx.repl.main.ut.Ut;

import java.util.Map;

public class JdtxStateOuterMailer implements Runnable {


    private IMailer mailer;
    private StateItemStack state;
    private long sleepDuration = 100;


    public JdtxStateOuterMailer(StateItemStackThread state, IMailer mailer) {
        this.state = state;
        this.mailer = mailer;
    }


    @Override
    public void run() {
        System.out.println("Started: " + this.getClass().getName() + ", mailer: " + mailer.getClass().getName());
        System.out.println("mailer.guid: " + ((MailerHttp) mailer).guid);

        //
        while (true) {
            try {
                ITreeNode<StateItem> root = state.getAll();
                Map<String, Object> stateValue = UtStateCnv.stateItemToMap(root);
                try {
                    mailer.setData(stateValue, "state.json", null);
                } catch (Exception e) {
                    String msg = Ut.getExceptionMessage(e);
                    System.out.println("JdtxStateOuterMailer.guid: " + ((MailerHttp) mailer).guid + ", error: " + msg);
                }

                //
                try {
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    String msg = Ut.getExceptionMessage(e);
                    System.out.println("JdtxStateOuterMailer.guid: " + ((MailerHttp) mailer).guid + ", error: " + msg);
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
