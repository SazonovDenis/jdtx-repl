package jdtx.repl.main.api.util;

public class UtJdxErrors {

    public static String collectExceptionText(Exception e) {
        String errText = e.toString();
        if (e.getCause() != null) {
            errText = errText + "\n" + e.getCause().toString();
        }
        return errText;
    }

    public static String message_replicaMailNotFound = "Replica not found, guid";
    public static String message_boxAlreadyExists = "Box already exists";
    public static String message_guidAlreadyExists = "Guid already exists";
    public static String message_replicaRecordNotFound = "replica record not found";
    public static String message_fileNotFound = "java.io.FileNotFoundException";
    public static String message_replicaDataFileNotExists = "replica.file not exists";
    public static String message_replicaBadCrc = "replica.file.crc <> replica.info.crc";
    public static String message_replicaZipError = "ZipException: invalid stored block lengths";
    public static String message_replicaNotFoundContent = "not found content in replica";
    public static String message_failedDatabaseLock = "database lock failed";

    public static boolean errorIs_replicaMailNotFound(Exception e) {
        return collectExceptionText(e).contains(message_replicaMailNotFound);
    }

    public static boolean errorIs_BoxAlreadyExists(Exception e) {
        return collectExceptionText(e).contains(message_boxAlreadyExists);
    }

    public static boolean errorIs_GuidAlreadyExists(Exception e) {
        return collectExceptionText(e).contains(message_guidAlreadyExists);
    }

    private static boolean errorIs_replicaFileNotFound(Exception e) {
        return collectExceptionText(e).contains(message_fileNotFound);
    }

    public static boolean errorIs_replicaFileNotExists(Exception e) {
        return collectExceptionText(e).contains(message_replicaDataFileNotExists);
    }

    public static boolean errorIs_replicaBadCrc(Exception e) {
        return collectExceptionText(e).contains(message_replicaBadCrc);
    }

    public static boolean errorIs_replicaFileZipError(Exception e) {
        return collectExceptionText(e).contains(message_replicaZipError);
    }

    public static boolean errorIs_replicaFileNotFoundContent(Exception e) {
        return collectExceptionText(e).contains(message_replicaNotFoundContent);
    }

    public static boolean errorIs_failedDatabaseLock(Exception e) {
        return collectExceptionText(e).contains(message_failedDatabaseLock);
    }

    /**
     * @return true, если физическая проблема с файлом
     */
    public static boolean errorIs_replicaFile(Exception e) {
        return (errorIs_replicaFileNotFound(e) ||
                errorIs_replicaFileNotExists(e) ||
                errorIs_replicaBadCrc(e) ||
                errorIs_replicaFileZipError(e) ||
                errorIs_replicaFileNotFoundContent(e)

        );
    }


}
