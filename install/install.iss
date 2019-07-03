#define App "JadatexSync"
#define DefaultDir "Jadatex.Sync"
#define AppName "Jadatex. Служба синхронизации"

#define OutputDir "Z:\jdtx-repl\install"
#define SourceDir "Z:\jdtx-repl\"
#define SourceDirJre "C:\jdk1.8.0_45\jre\"



#define AppVersion "_CFG_APP_VERSION"
#define _CFG_INSTALL_JRE

#ifdef installJre
  #define FilenameSuffixJre = ".jre"
#else
  #define FilenameSuffixJre = ""
#endif



[Setup]
AppID=Jadatex.Sync
AppPublisher=Компания Jadatex
AppPublisherURL=http://jadatex.com
AppSupportURL=http://jadatex.com/wiki/contacts
AppUpdatesURL=http://jadatex.com/wiki/downloads_list

OutputBaseFilename={#App}-{#AppVersion}{#FilenameSuffixJre}
AppName={#AppName}
AppVerName={#App} (Версия {#AppVersion})
DefaultDirName={commondocs}\{#DefaultDir}
DefaultGroupName=Jadatex PawnShop

OutputDir={#OutputDir}
SourceDir={#SourceDir}

;WizardImageFile=Images\PawnshopApp_install.bmp
;WizardSmallImageFile=Images\PawnshopApp_install_48.bmp
WizardImageStretch=false

DisableReadyPage=true
DisableStartupPrompt=true
UninstallRestartComputer=false
DisableProgramGroupPage=true
;DisableDirPage=true
DisableFinishedPage=true
AlwaysShowComponentsList=true

Compression=lzma/ultra
SolidCompression=true
InternalCompressLevel=ultra
;SetupIconFile=Images\PawnshopApp_install_icon.ico

DirExistsWarning=no

PrivilegesRequired=lowest

[Languages]
Name: ru; MessagesFile: compiler:Languages\Russian.isl

[Icons]
;Name: {group}\{#AppName}; Filename: {app}\Pawn.exe; WorkingDir: {app}
;Name: {commondesktop}\{#AppName}; Filename: {app}\Pawn.exe; WorkingDir: {app}; IconIndex: 0

[_ISTool]
EnableISX=true

[LangOptions]
LanguageID=$313D



[Dirs]
;Name: {commondocs}\Jadatex.PawnShop; Flags: uninsneveruninstall
;Name: {commondocs}\Jadatex.PawnShop\ReportCustom; Flags: uninsneveruninstall
;Name: {commondocs}\Jadatex.PawnShop\Install; Flags: uninsneveruninstall



[Files]
Source: temp\distr\*.*; DestDir: {app}; Flags: ignoreversion recursesubdirs createallsubdirs
#ifdef installJre
Source: {#SourceDirJre}\*.*; DestDir: {app}\jre; Flags: ignoreversion recursesubdirs createallsubdirs
#endif



Source: install\cm.bat;                        DestDir: {app};                 Flags: ignoreversion
Source: install\jc-run.bat;                    DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\jc-run.vbs;                    DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\sample.log.properties;         DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\cfg\decode_strategy.json;      DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\publication_full_152.json; DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\publication_full_163.json; DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\publication_null.json;     DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\sample.srv.json;           DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\sample.ws.json;            DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion

Source: install\sample._app.rt;                DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _app.rt; 
Source: install\sample._db-ini.rt;             DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _db-ini.rt
Source: install\sample.log.properties;         DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: log.properties
Source: install\cfg\sample.ws.json;            DestDir: {app}\web\WEB-INF\cfg; Flags: onlyifdoesntexist; DestName: ws.json



[Run]
;Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}
;Filename: jc.bat; Parameters: repl-service-install; WorkingDir: {app}


[UninstallRun]
Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}



[UninstallDelete]
Name: {app}\output.err; Type: files
Name: {app}\output.msg; Type: files
Name: {app}\web\WEB-INF\cfg\srv.json; Type: files


[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=Перед началом установки рекомендуется закрыть все прочие программы.
ClickNext=Нажмите «Далее», чтобы установить версию _CFG_APP_VERSION, или «Отмена», чтобы отменить установку.

StatusRunProgram=Заключительные шаги установки...

UninstalledAll=Программа %1 была удалена с Вашего компьютера.%nИз соображений сохранности данных файлы рабочие каталоги (web\WEB-INF\*) не были удалены. Вы можете удалить их самостоятельно.



[Code]

procedure CurStepChanged(CurStep: TSetupStep); 
var
  resultCode: Integer;
  i: Integer;
begin
  if (CurStep=ssInstall) then
  begin
    Exec(ExpandConstant('jc.bat'), 'repl-service-stop', ExpandConstant('{app}'), SW_SHOW, ewWaitUntilTerminated, resultCode);
    Exec(ExpandConstant('jc.bat'), 'repl-service-remove', ExpandConstant('{app}'), SW_SHOW, ewWaitUntilTerminated, resultCode);
  end;

  if (CurStep=ssDone) then
  begin
    for i:=1 to ParamCount() do
    begin
      if Lowercase(ParamStr(i)) = '/repl-service-install' then
      begin
        Exec(ExpandConstant('jc.bat'), 'repl-service-install', ExpandConstant('{app}'), SW_SHOW, ewWaitUntilTerminated, resultCode);
        Exec(ExpandConstant('jc.bat'), 'repl-service-start', ExpandConstant('{app}'), SW_SHOW, ewWaitUntilTerminated, resultCode);
        exit;
      end;
    end;

    //MsgBox('repl-service-remove', mbInformation, MB_OK);
    Exec(ExpandConstant('jc.bat'), 'repl-service-remove', ExpandConstant('{app}'), SW_SHOW, ewWaitUntilTerminated, resultCode);
  end;
end;

