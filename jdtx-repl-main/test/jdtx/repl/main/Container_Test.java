package jdtx.repl.main;

import com.jdtx.state.*;
import com.jdtx.tree.*;
import jdtx.repl.main.log.*;
import org.apache.commons.io.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

public class Container_Test {

    TreeItemContainer state = LogContainer.state;

    @Test
    public void test_wait() throws Exception {
        System.out.println("TreeItemContainer");

        // Простейший одиночный counter - подчиненные процессы
        //TreeItemContainer state = new TreeItemContainer();
        pauseAndPrintFile(state.getRoot(), 1);

        state.start();
        state.current().setValue("name", "task #1");
        pauseAndPrintFile(state.getRoot(), 1);
        for (int i = 0; i < 10; i++) {
            state.current().incValue("count", 1);
            pauseAndPrintFile(state.getRoot(), 1);
        }

        // Запуск дочернего процесса - спуск вглубь на 2-й уровень
        state.start();
        state.current().setValue("name", "task #2");
        state.current().incValue("total", 200);
        pauseAndPrintFile(state.getRoot(), 1);
        for (int i = 0; i < 20; i++) {
            state.current().incValue("count", 10);
            pauseAndPrintFile(state.getRoot(), 1);
        }

        // Остановка дочернего процесса - подъем со 2-го на 1-й уровень
        state.stop();
        pauseAndPrintFile(state.getRoot(), 1);

        pauseAndPrintFile(state.getRoot(), 1);

        state.stop();
        pauseAndPrintFile(state.getRoot(), 1);
    }

    private void pauseAndPrintFile(ITreeNode<StateItem> root, int n) throws Exception {
        Thread.sleep(200);

        Map map = UtStateCnv.stateItemToMap(root);
        JSONObject jsonObject = new JSONObject(map);
        System.out.println(jsonObject.toJSONString());

        FileUtils.writeStringToFile(new File("temp/" + n + ".json"), jsonObject.toJSONString());
    }

}
