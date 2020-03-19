package jdtx.repl.main.api.que;

import jandcode.utils.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;

import java.io.*;

public class JdxQueFile {

    //
    String queType;
    String baseDir;

    public long getMaxNoFromDir() throws Exception {
        String inFileMask = "*.zip";
        File dir = new File(baseDir);
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));

        //
        long idx = 0;
        for (File file : files) {
            if (idx < getNo(file.getName())) {
                idx = getNo(file.getName());
            }
        }

        //
        return idx;
    }

    public IReplica readByNoFromDir(long no) throws Exception {
        IReplica replica = new ReplicaFile();
        //
        String actualFileName = genFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        replica.setFile(actualFile);
        //
        JdxReplicaReaderXml.readReplicaInfo(replica);
        //
        return replica;
    }

    long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }

    String genFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }


}
