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

Source: web\WEB-INF\sample._app.rt; DestDir: {app}\web\WEB-INF; DestName: _app.rt; Flags: onlyifdoesntexist
Source: web\WEB-INF\sample._db-ini.rt; DestDir: {app}\web\WEB-INF; Flags: onlyifdoesntexist; DestName: _db-ini.rt
Source: web\WEB-INF\sample.log.properties; DestDir: {app}\web\WEB-INF; Flags: onlyifdoesntexist; DestName: log.properties
;Source: web\WEB-INF\cfg\sample.srv.json; DestDir: {app}\web\WEB-INF\cfg\srv.json; Flags: onlyifdoesntexist
Source: web\WEB-INF\cfg\sample.ws.json; DestDir: {app}\web\WEB-INF\cfg; Flags: onlyifdoesntexist; DestName: ws.json
Source: install\cm.bat; DestDir: {app}; DestName: cm.bat; Flags: onlyifdoesntexist



[Run]
Filename: SCHTASKS; Parameters: /Delete /TN JadatexSync /f; WorkingDir: {app}
Filename: SCHTASKS; Parameters: "/Create /TN JadatexSync /TR "\"{app}\start.vbs"" /SC MINUTE /MO 5"; WorkingDir: {app}
установка рабочего каталога через xml


[UninstallRun]
Filename: SCHTASKS; Parameters: /Delete /TN JadatexSync /f; WorkingDir: {app}



[UninstallDelete]
Name: {app}\output.err; Type: files
Name: {app}\output.msg; Type: files



[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=Перед началом установки рекомендуется закрыть все прочие программы.
ClickNext=Нажмите «Далее», чтобы установить версию _CFG_APP_VERSION, или «Отмена», чтобы отменить установку.

StatusRunProgram=Заключительные шаги установки...

UninstalledAll=Программа %1 была удалена с Вашего компьютера.%nИз соображений сохранности данных файлы рабочие каталоги (web\WEB-INF\*) не были удалены. Вы можете удалить их самостоятельно.



[Code]

function InitializeSetup(): Boolean;
var
  resultCode: Integer;
  ExecOk: boolean;
begin
  Result:=true;
  Exec(ExpandConstant('SCHTASKS'), '/Delete /TN JadatexSync /f', '', SW_SHOW, ewWaitUntilTerminated, resultCode);
end;