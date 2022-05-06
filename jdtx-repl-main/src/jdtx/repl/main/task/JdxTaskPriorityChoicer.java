package jdtx.repl.main.task;

import jandcode.bgtasks.BgTask;
import jandcode.bgtasks.BgTasksChoicer;

import java.util.Collection;

/**
 */
public class JdxTaskPriorityChoicer extends BgTasksChoicer {

    @Override
    public BgTask choiceNextTask(Collection<BgTask> tasks, Collection<BgTask> runnedTask) throws Exception {
        for (BgTask x : tasks) {
            if (x.getName().equals("ws")) {
                if (((BgTaskWsRepl) x).runImmediate == true) {
                    return x;
                }
            }
        }

        return null;
    }

}
