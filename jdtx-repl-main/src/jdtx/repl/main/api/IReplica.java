package jdtx.repl.main.api;

import java.io.*;

/**
 * Блок данных
 */
public interface IReplica {

    void setAge(long age);

    long getAge();

    void setFile(File file);

    File getFile();

}
