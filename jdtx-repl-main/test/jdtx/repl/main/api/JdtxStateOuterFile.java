package jdtx.repl.main.api;

import com.jdtx.state.StateItem;
import com.jdtx.state.StateItemStack;
import com.jdtx.state.UtStateCnv;
import com.jdtx.state.impl.StateItemStackThread;
import com.jdtx.tree.ITreeNode;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.Map;

public class JdtxStateOuterFile implements Runnable {


    private String fileName;
    private StateItemStack stateItemStack;
    private long sleepDuration = 100;


    public JdtxStateOuterFile(StateItemStackThread stateItemStack, String fileName) {
        this.fileName = fileName;
        this.stateItemStack = stateItemStack;
    }


    @Override
    public void run() {
        File outFile = new File(fileName);
        System.out.println("Started: " + this.getClass().getName() + ", file: " + outFile.getAbsolutePath());

        //
        while (true) {
            try {
                ITreeNode<StateItem> root = stateItemStack.getAll();

                //
                Map map = UtStateCnv.stateItemToMap(root);
                JSONObject jsonObject = new JSONObject(map);
                FileUtils.writeStringToFile(outFile, jsonObject.toJSONString());

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
