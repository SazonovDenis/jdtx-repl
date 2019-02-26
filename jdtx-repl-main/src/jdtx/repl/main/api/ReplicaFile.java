package jdtx.repl.main.api;

import java.io.*;

/**
 * Реплика в файле.
 */
public class ReplicaFile implements IReplica {

    File file = null;
    long wsId = -1;
    long age = -1;

    public long getWsId() {
        return wsId;
    }

    public void setWsId(long wsId) {
        this.wsId = wsId;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public File getFile() {
        return this.file;
    }

    public static void readReplicaInfo(IReplica replica) throws Exception {
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica);
        replica.setWsId(reader.getWsId());
        replica.setAge(reader.getAge());
        reader.close();
    }

}
