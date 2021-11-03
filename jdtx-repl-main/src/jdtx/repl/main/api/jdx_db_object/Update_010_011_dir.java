package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.que.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;

import java.io.*;

/**
 * Смена способа хранения реплик:
 * теперь реплики хранятся не все кучей в одном каталоге, а в подкаталогах, по тысяче штук
 */
public class Update_010_011_dir implements ISqlScriptExecutor {

    protected Log log = LogFactory.getLog("jdtx.Update_010_011_dir");

    @Override
    public void exec(Db db) throws Exception {
        String dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);

        // Сервер
        String dataRootSrv = dataRoot + "srv/";
        //
        if (new File(dataRootSrv).exists()) {
            // Сервер: queCommon
            String queCommon_Dir = dataRootSrv + "que_common/";
            convertDir(queCommon_Dir);

            // Сервер: рабочие каталоги мейлеров
            for (int wsId = 1; wsId < 50; wsId++) {
                String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
                //
                String que_out000_Dir = dataRootSrv + "que_out000_ws_" + sWsId + "/";
                String que_out001_Dir = dataRootSrv + "que_out001_ws_" + sWsId + "/";
                //
                if (new File(que_out000_Dir).exists()) {
                    convertDir(que_out000_Dir);
                }
                if (new File(que_out001_Dir).exists()) {
                    convertDir(que_out001_Dir);
                }
            }
        }

        // Рабочие станции
        for (int wsId = 1; wsId < 50; wsId++) {
            String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
            String dataRootWs = dataRoot + "ws_" + sWsId + "/";
            //
            if (new File(dataRootWs).exists()) {
                String que_in_Dir = dataRootWs + "que_in/";
                String que_in001_Dir = dataRootWs + "que_in001/";
                String que_out_Dir = dataRootWs + "que_out/";
                //
                convertDir(que_in_Dir);
                convertDir(que_in001_Dir);
                convertDir(que_out_Dir);
            }
        }
    }

    protected void convertDir(String dirName) throws IOException {
        log.info("convertDir: " + dirName);

        //
        dirName = UtFile.unnormPath(dirName) + "/";
        File dir = new File(dirName);

        //
        if (!dir.exists()) {
            throw new XError("Dir not exists: " + dir.getCanonicalPath());
        }

        //
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter("*.zip", IOCase.INSENSITIVE));
        int n = 0;
        for (File file : files) {
            convertFile(dirName, file.getName());
            //
            n++;
            if (n % 1000 == 0) {
                log.info("  " + n + "/" + files.length + ", " + file.getName());
            }
        }

        //
        log.info("  move done: " + files.length);
    }

    private void convertFile(String baseDirName, String fileNameOld) throws IOException {
        long no = getNo_ver10(fileNameOld);
        String fileNameNew = JdxStorageFile.getFileName(no);

        //
        String filePathOld = baseDirName + fileNameOld;
        String filePathNew = baseDirName + fileNameNew;
        File fileOld = new File(filePathOld);
        File fileNew = new File(filePathNew);

        //
        UtFile.mkdirs(fileNew.getParent());

        //
        FileUtils.moveFile(fileOld, fileNew);

        //
        //log.info("  move: " + fileOld.getCanonicalPath() + " -> " + fileNew.getCanonicalPath());
    }

    long getNo_ver10(String fileName) {
        return Long.parseLong(fileName.substring(0, 9));
    }

}
