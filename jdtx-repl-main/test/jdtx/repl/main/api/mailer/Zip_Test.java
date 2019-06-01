package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.test.*;
import org.junit.*;

import java.io.*;
import java.util.zip.*;

public class Zip_Test extends UtilsTestCase {

    @Test
    public void test_putToZip() throws Exception {
        File[] fIn = {
                new File("../_test-data/idea.2018.02.07.zip"),
                new File("../_test-data/jdk-1.8.0_202.zip")
        };
        File fZip = new File("../_test-data/test.zip");
        //
        zip(fIn, fZip);
        //
        System.out.println(fZip);
    }

    @Test
    public void test_zipReadHead() throws Exception {
        File fZip = new File("../_test-data/test.zip");
        //
        printZipContent(fZip);
    }

    void zip(File[] fInArr, File fZip) throws IOException {
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];

        String outFileName = fZip.getPath();

        try (
                FileOutputStream outputStream = new FileOutputStream(outFileName);
                ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        ) {
            for (File fIn : fInArr) {

                StopWatch sw = new StopWatch();
                sw.start();

                String inFilename = fIn.getPath();
                FileInputStream inputStream = new FileInputStream(inFilename);
                System.out.println(inFilename);

                // ---
                ZipEntry zipEntry_head = new ZipEntry(fIn.getName() + ".json");
                zipOutputStream.putNextEntry(zipEntry_head);
                String head = "{size: \"" + fIn.length() + "\"}";
                zipOutputStream.write(head.getBytes());
                //
                zipOutputStream.closeEntry();


                // ---
                ZipEntry zipEntry = new ZipEntry(fIn.getName());
                zipOutputStream.putNextEntry(zipEntry);

                //
                long done = 0;
                long done_printed = 0;
                while (inputStream.available() > 0) {
                    int n = inputStream.read(buffer);
                    zipOutputStream.write(buffer, 0, n);
                    done = done + n;
                    //
                    if (done - done_printed > 1024 * 1024 * 10) {
                        done_printed = done;
                        System.out.print("\r" + done / 1024 + " Kb");
                    }
                }

                // закрываем текущую запись для новой записи
                zipOutputStream.closeEntry();

                //
                System.out.println("\r" + done / 1024 + " Kb done");
                sw.stop();

            }
        }
    }


    void printZipContent(File fZip) throws Exception {
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];


        try (
                ZipInputStream inputStream = new ZipInputStream(new FileInputStream(fZip))
        ) {
            System.out.println(fZip);

            ZipEntry entry;
            while ((entry = inputStream.getNextEntry()) != null) {
                StopWatch sw = new StopWatch();
                sw.start();

                //
                String name = entry.getName(); // получим название файла
                long size = entry.getSize();  // получим его размер в байтах
                System.out.printf("%s, size: %d\n", name, size);

                if (name.endsWith(".json")) {
                    // распаковка
                    String outFileName = fZip.getParent() + "/" + name;
                    FileOutputStream outputStream = new FileOutputStream(outFileName);
                    //
                    for (int n = inputStream.read(buffer); n > 0; n = inputStream.read(buffer)) {
                        outputStream.write(buffer, 0, n);
                    }
                    //
                    outputStream.close();

                    if (name.endsWith(".json")) {
                        String s = UtFile.loadString(outFileName);
                        System.out.println(s);
                    }
                }


                //
                inputStream.closeEntry();

                //
                sw.stop();
                System.out.println("");
            }
        }
    }


}
