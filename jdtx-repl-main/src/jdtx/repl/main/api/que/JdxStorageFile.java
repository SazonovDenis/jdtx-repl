package jdtx.repl.main.api.que;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.io.*;

/**
 * Хранилище реплик.
 * Реализация интерфейса IJdxReplicaStorage
 */
public class JdxStorageFile implements IJdxReplicaStorage, IJdxStorageFile {


    //
    protected static Log log = LogFactory.getLog("jdtx.JdxStorageFile");


    //
    String baseDir;


    /*
     * IJdxStorageFile
     */

    @Override
    public String getBaseDir() {
        return baseDir;
    }

    @Override
    public void setDataRoot(String dataRoot) {
        if (dataRoot == null || dataRoot.length() == 0) {
            throw new XError("Invalid dataRoot");
        }
        //
        this.baseDir = UtFile.unnormPath(dataRoot) + "/";
        //
        UtFile.mkdirs(getBaseDir());
    }

    /*
     * IJdxReplicaStorage
     */

    @Override
    public void put(IReplica replica, long no) throws Exception {
        // Проверки: правильность полей реплики
        UtJdx.validateReplicaFields(replica);

        // Переносим файл на постоянное место
        String actualFileName = getFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        File replicaFile = replica.getFile();

        // Файл должен быть - иначе незачем делать put
        if (replicaFile == null) {
            throw new XError("Invalid replica.file == null");
        }

        // Если файл, указанный у реплики не совпадает с постоянным местом хранения, то файл переносим на постоянное место.
        if (replicaFile.getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            // Если какой-то РАЗЛИЧАЮЩИЙСЯ файл уже занимает постоянное место, то этот файл НЕ удаляем.
            if (actualFile.exists()) {
                String replicaFileMd5 = UtJdx.getMd5File(replicaFile);
                String actualFileMd5 = UtJdx.getMd5File(actualFile);
                if (replicaFileMd5.compareToIgnoreCase(actualFileMd5) != 0) {
                    // Если ДРУГОЙ файл уже занимает постоянное место, то этот файл НЕ удаляем.
                    throw new XError("Other actualFile already exists: " + actualFile.getAbsolutePath());
                } else {
                    // Если ТАКОЙ-ЖЕ файл уже занимает постоянное место, то этот файл можно заменить.
                    log.warn("Same actualFile already exists: " + actualFile.getAbsolutePath() + ", delete existing");
                    actualFile.delete();
                }
            }
            // Переносим файл на постоянное место
            FileUtils.moveFile(replicaFile, actualFile);
        }
    }

    @Override
    public IReplica get(long no) throws Exception {
        IReplica replica = new ReplicaFile();
        //
        String actualFileName = getFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        replica.setFile(actualFile);
        //
        // JdxReplicaReaderXml.readReplicaInfo(replica);
        //
        return replica;
    }


    /*
     * Утилиты
     */

    public long getMaxNoFromDir() {
        File dir = new File(baseDir);

        if (!dir.exists()) {
            throw new XError("Dir not exists: " + dir.getAbsolutePath());
        }

        //
        String inFileMask = "*.zip";
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

    private long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }

    public String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }


}
