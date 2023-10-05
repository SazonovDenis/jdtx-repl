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
    private static long dirSize = 1000;

    //
    String baseDir;


    //
    protected static Log log = LogFactory.getLog("jdtx.StorageFile");

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

    @Override
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


    /*
     * IJdxReplicaStorage
     */

    @Override
    public void put(IReplica replica, long no) throws Exception {
        // Проверки: правильность полей реплики
        UtJdx.validateReplicaFields(replica);

        // Переносим файл на постоянное место
        String replicaFileName = getFileName(no);
        File oldFile = new File(baseDir + replicaFileName);
        //
        File newFile = replica.getData();

        // Файл должен быть - иначе незачем делать put
        if (newFile == null) {
            throw new XError("New replica file == null");
        }

        // Если файл, указанный у реплики не совпадает с постоянным местом хранения, то файл переносим на постоянное место.
        if (newFile.getCanonicalPath().compareTo(oldFile.getCanonicalPath()) != 0) {
            // Какой-то файл уже занимает постоянное место?
            if (oldFile.exists()) {
                log.warn("Replace file: " + oldFile.getAbsolutePath());
                //
                String crcInfo = replica.getInfo().getCrc();
                String crcNewFile = UtJdx.getMd5File(newFile);
                String crcOldFile = UtJdx.getMd5File(oldFile);
                log.warn("New info crc: " + crcInfo);
                log.warn("New file crc: " + crcNewFile + ", length: " + newFile.length());
                log.warn("Old file crc: " + crcOldFile + ", length: " + oldFile.length());
                //
                // Удаялем старый файл
                if (!oldFile.delete()) {
                    throw new IOException("Unable to delete file: " + oldFile.getAbsolutePath());
                }
            }
            // Переносим файл на постоянное место
            FileUtils.moveFile(newFile, oldFile);
            // Пусть в реплике теперь указан правильный файл
            replica.setData(oldFile);
        }
    }

    @Override
    public void remove(long no) throws Exception {
        log.info("Remove replica from storage, no: " + no + ", baseDir: " + baseDir);
        //
        String actualFileName = JdxStorageFile.getFileName(no);
        File actualFile = new File(baseDir + actualFileName);
        //
        if (actualFile.exists() && !actualFile.delete()) {
            throw new XError("Unable to remove replica: " + actualFile.getAbsolutePath());
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
        replica.getInfo().setNo(no);
        //
        return replica;
    }


    /*
     * Утилиты
     */

    public static long getNo(String fileName) {
        return Long.parseLong(fileName.substring(fileName.length() - 13, fileName.length() - 4));
    }

    public static String getFileName(long no) {
        long noDir = no / dirSize;
        return UtString.padLeft(String.valueOf(noDir), 6, '0') + "/" + UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }


}
