package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Блок данных.
 */
public interface IReplica {

    IReplicaInfo getInfo();

    File getData();

    void setData(File file);

}
