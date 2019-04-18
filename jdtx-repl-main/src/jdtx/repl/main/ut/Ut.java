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

    public static String getStackTrace(Exception e) {
        StringWriter swr = new StringWriter();
        PrintWriter wr = new PrintWriter(swr);
        e.printStackTrace(wr);
        return swr.getBuffer().toString();
    }

    public static String getExceptionMessage(Exception e) {
        String msg;
        if (e.getCause() != null) {
            msg = e.getCause().getMessage();
        } else {
            msg = e.getMessage();
        }
        if (msg == null) {
            msg = e.toString();
        }
        return msg;
    }



}
