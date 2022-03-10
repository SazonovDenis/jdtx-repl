package jdtx.repl.main.api.replica;

import java.io.*;
import java.util.zip.*;

public class JdxReplicaFileInputStream extends ZipInputStream {

    public FileInputStream fileInputStream;

    public JdxReplicaFileInputStream(FileInputStream fileInputStream) {
        super(fileInputStream);
        this.fileInputStream = fileInputStream;
    }

    public long getSize() throws IOException {
        return fileInputStream.getChannel().size();
    }

    public long getPos() throws IOException {
        return fileInputStream.getChannel().position();
    }

}
