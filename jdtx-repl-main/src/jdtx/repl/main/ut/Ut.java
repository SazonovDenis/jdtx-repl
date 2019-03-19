package jdtx.repl.main.ut;

import java.io.*;
import java.util.*;

/**
 */
public class Ut {

    private static void copyFileUsingStream(File source, File dest, IListener listener) throws IOException {
        InputStream is = null;
        OutputStream os = null;
        Map info = new HashMap<>();
        long total = 0;

        //
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024 * 16];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
                //
                if (listener != null) {
                    total = total + length;
                    info.put("total", total);
                    listener.onEventInfo(info);
                }
            }
        } finally {
            is.close();
            os.close();
        }
    }

}
