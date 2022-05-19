package jdtx.repl.main.api.util;

public class UtJdxErrors {

    public static String collectExceptionText(Exception e) {
        String errText = e.toString();
        if (e.getCause() != null) {
            errText = errText + "\n" + e.getCause().toString();
        }
        return errText;
    }

    public static String message_replicaBadCrc = "replica.file.crc <> replica.info.crc";
    public static String message_replicaFileNotExists = "replica.file not exists";
    public static String message_replicaNotFoundContent = "Not found content in replica";
    public static String message_replicaNotFound = "Replica not found";
    public static String message_replicaZipError = "ZipException: invalid stored block lengths";

    public static boolean errorIs_replicaBadCrc(Exception e) {
        return collectExceptionText(e).contains(message_replicaBadCrc);
    }

    public static boolean errorIs_replicaFileNotExists(Exception e) {
        return collectExceptionText(e).contains(message_replicaFileNotExists);
    }

    public static boolean errorIs_replicaNotFoundContent(Exception e) {
        return collectExceptionText(e).contains(message_replicaNotFoundContent);
    }

    public static boolean errorIs_MailerReplicaNotFound(Exception e) {
        return collectExceptionText(e).contains(message_replicaNotFound);
    }

    public static boolean errorIs_replicaZipError(Exception e) {
        return collectExceptionText(e).contains(message_replicaZipError);
    }

}
