package jdtx.repl.main.api;

import jandcode.utils.io.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 *
 */
public class UtZip {

    //
    protected static Log log = LogFactory.getLog("UtZip");


    public static void doZipFiles(Collection<File> files, File rootDir, File destFile) throws Exception {
        String rootDirName = rootDir.getCanonicalPath();
        int rootDirNameLen = rootDirName.length();

        //
        log.info("Zip dir: " + rootDirName + " to file: " + destFile.getCanonicalPath());

        //
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];

        try (
                FileOutputStream outputStream = new FileOutputStream(destFile);
                ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        ) {
            for (File file : files) {
                //StopWatch sw = new StopWatch();
                //sw.start();

                String fileName = file.getCanonicalPath();
                FileInputStream inputStream = new FileInputStream(fileName);
                log.info(fileName);

                if (!fileName.startsWith(rootDirName)) {
                    throw new Exception("file name: " + fileName + " not startsWith: " + rootDirName);
                }
                String fileNameInZip = fileName.substring(rootDirNameLen + 1);

                // ---
                ZipEntry zipEntry = new ZipEntry(fileNameInZip);
                zipOutputStream.putNextEntry(zipEntry);

                //
                long countDone = 0;
                long done_printed = 0;
                while (inputStream.available() > 0) {
                    int count = inputStream.read(buffer);
                    zipOutputStream.write(buffer, 0, count);
                    countDone = countDone + count;
                    //
                    if (countDone - done_printed > 1024 * 1024 * 10) {
                        done_printed = countDone;
                        log.info(countDone / 1024 + " Kb");
                    }
                }

                // закрываем текущую запись для новой записи
                zipOutputStream.closeEntry();

                //
                if (done_printed != 0) {
                    log.info(countDone / 1024 + " Kb done");
                }
                //sw.stop();

            }
        }
    }


    public static void doZipDir(String dirName, String destFileName) throws Exception {
        DirScannerLocal scanner = new DirScannerLocal();
        scanner.setRecursive(true);
        scanner.setRootDir(dirName);
        scanner.scan();

        File destFile = new File(destFileName);
        doZipFiles(scanner.getFiles(), new File(dirName), destFile);
    }


    public static void doUnzipDir(String zipFileName, String destDirName) throws IOException {
        File dir = new File(destDirName);

        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();

        //
        FileInputStream fis = new FileInputStream(zipFileName);

        //buffer for read and write data to file
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(fis);

        ZipEntry ze = zis.getNextEntry();
        while (ze != null) {
            if (!ze.isDirectory()) {
                String fileName = ze.getName();
                File newFile = new File(destDirName + File.separator + fileName);

                log.info("Unzipping to " + newFile.getAbsolutePath());

                //create directories for sub directories in zip
                new File(newFile.getParent()).mkdirs();

                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
            }
            ze = zis.getNextEntry();
        }

        //close last ZipEntry
        zis.closeEntry();
        zis.close();
        fis.close();
    }


}
