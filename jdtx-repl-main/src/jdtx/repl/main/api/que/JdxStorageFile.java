package jdtx.repl.main.api.que;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;

import java.io.*;

/**
 * Хранилище реплик.
 * Реализация интерфейса IJdxReplicaStorage
 * <p>
 * Реплики хранятся в подкаталогах каталога baseDir, по dirSize штук
 */
public class JdxStorageFile implements IJdxReplicaStorage, IJdxStorageFile {


    //
    protected static Log log = LogFactory.getLog("jdtx.JdxStorageFile");

    //
    private static long dirSize = 1000;

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
        //
        File replicaFile = replica.getData();

        // Файл должен быть - иначе незачем делать put
        if (replicaFile == null) {
            throw new XError("Invalid replica.file == null");
        }

        // Если файл, указанный у реплики не совпадает с постоянным местом хранения, то файл переносим на постоянное место.
        if (replicaFile.getCanonicalPath().compareTo(actualFile.getCanonicalPath()) != 0) {
            // Если какой-то РАЗЛИЧАЮЩИЙСЯ файл уже занимает постоянное место, то этот файл НЕ удаляем.
            if (actualFile.exists()) {
                String replicaFileMd5 = replica.getInfo().getCrc();
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
            // Пусть в реплике теперь указан правильный файл
            replica.setData(actualFile);
        }
    }

    @Override
    public IReplica get(long no) throws Exception {
        IReplica replica = new ReplicaFile();
        //
        String actualFileName = getFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        replica.setData(actualFile);
        //
        return replica;
    }


    /*
     * Утилиты
     */

    public long getMaxNoFromDir() {
        File dirBase = new File(baseDir);

        if (!dirBase.exists()) {
            throw new XError("Dir not exists: " + dirBase.getAbsolutePath());
        }

        //
        long idxFile = 0;

        // Ищем максимальный каталог
        String inFileMask = UtString.repeat("?", 6);
        File[] dirs = dirBase.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
        long idxDir = -1;
        File dirMaxIdx = null;
        for (File dir : dirs) {
            if (idxDir < Long.parseLong(dir.getName())) {
                idxDir = Long.parseLong(dir.getName());
                dirMaxIdx = dir;
            }
        }
        // Каталог dirBase пуст
        if (dirMaxIdx == null) {
            return idxFile;
        }

        // Ищем максимальный файл в максимальном каталоге
        inFileMask = "*.zip";
        File[] files = dirMaxIdx.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
        for (File file : files) {
            if (idxFile < getNo(file.getName())) {
                idxFile = getNo(file.getName());
            }
        }

        //
        return idxFile;
    }

    public static long getNo(String fileName) {
        return Long.parseLong(fileName.substring(fileName.length() - 13, fileName.length() - 4));
    }

    public static String getFileName(long no) {
        long noDir = no / dirSize;
        return UtString.padLeft(String.valueOf(noDir), 6, '0') + "/" + UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }


}
