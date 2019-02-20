package jdtx.repl.main.api;

import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;

import java.io.*;
import java.util.*;

/**
 * Читатель сообщений с репликами, формирователь входящей очереди
 */
public class JdxQueReaderDir {

    String baseFilePath = null;
    String inFileMask = "*.xml";

    /**
     * Забираем из внешнего источника и кладем во входящую очередь
     */
    void reloadDir(IJdxQue queIn) throws Exception {
        File inDir = new File(baseFilePath);
        File[] files = inDir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
        Arrays.sort(files);

        //
        for (File file : files) {
            IReplica replica = new ReplicaFile();
            replica.setFile(file);
            fillReplicaAgeFromFile(replica);
            //
            queIn.put(replica);
        }

    }

    private void fillReplicaAgeFromFile(IReplica replica) throws Exception {
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica);
        replica.setAge(reader.getAge());
        System.out.println("DbId = " + reader.getDbId());
        System.out.println("Age = " + reader.getAge());
        reader.close();
    }

}
