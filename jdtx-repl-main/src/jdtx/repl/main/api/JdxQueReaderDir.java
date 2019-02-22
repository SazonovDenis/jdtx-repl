package jdtx.repl.main.api;

import jandcode.utils.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * Читатель сообщений с репликами, формирователь входящей очереди
 * todo: надо ли? Что за клас????
 */
public class JdxQueReaderDir {

    String baseDir = null;
    String inFileMask = "*.xml";

    //
    protected static Log log = LogFactory.getLog("jdtx");

    /**
     * Забираем из внешнего источника и кладем во входящую очередь
     * todo: сделать помещение в очередь сразу после копирования реплики с сервера
     */
    void loadFromDirToQueIn(IJdxQueCommon queIn) throws Exception {
        log.info("loadFromDirToQueIn");

        // Узнаем что там у нас пришло во входящем каталоге?
        long dirAvailableNo = getDirMaxNo();

        // Узнаем что мы уже обработали?
        long queInDoneNo = queIn.getMaxNo();

        //
        log.info("loadFromDirToQueIn, self.queIn: " + queInDoneNo + ", dirAvailableNo: " + dirAvailableNo);

        //
        for (long no = queInDoneNo + 1; no <= dirAvailableNo; no++) {
            File file = new File(baseDir + getFileName(no));
            //
            IReplica replica = new ReplicaFile();
            replica.setFile(file);
            replica.setNo(getNo(file.getName()));
            readReplicaInfo(replica);
            //
            queIn.put(replica);
        }

    }

    private long getDirMaxNo() {
        File inDir = new File(baseDir);
        File[] files = inDir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
        Arrays.sort(files);
        if (files.length == 0) {
            return -1;
        } else {
            File lastFile = files[files.length - 1];
            return getNo(lastFile.getName());
        }
    }

    private void readReplicaInfo(IReplica replica) throws Exception {
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica);
        replica.setDbId(reader.getDbId());
        replica.setAge(reader.getAge());
        reader.close();
    }

    long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

}
