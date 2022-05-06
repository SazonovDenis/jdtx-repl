#define App "JadatexSync"
#define DefaultDir "Jadatex.Sync"
#define AppName "Jadatex. Служба синхронизации"

#define OutputDir "Z:\jdtx-repl\install"
#define SourceDir "Z:\jdtx-repl\"
#define SourceDirJre "D:\jdk1.8.0_45\jre\"



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
DisableDirPage=false
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



Source: install\cm.bat;                                 DestDir: {app};                 Flags: ignoreversion
Source: install\cm.bat.lnk;                             DestDir: {app};                 Flags: ignoreversion
Source: install\jc-start.bat;                           DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist
Source: install\jc-start.vbs;                           DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\jc-stop.bat;                            DestDir: {app}\web\WEB-INF;     Flags: ignoreversion
Source: install\jc-stop.vbs;                            DestDir: {app}\web\WEB-INF;     Flags: ignoreversion

Source: install\cfg\ws.json;                            DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion
Source: install\cfg\field_groups_194.json;              DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\decode_strategy_194.json;           DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_srv.json;       DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_ws.json;        DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent
Source: install\cfg\publication_lic_194_snapshot.json;  DestDir: {app}\web\WEB-INF\cfg; Flags: ignoreversion; Components: ServerComponent

Source: install\sample.srv._app.rt;                     DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _app.rt; Components: ServerComponent
Source: install\sample.ws._app.rt;                      DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _app.rt; Components: ClientComponent
Source: install\sample._db-ini.rt;                      DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: _db-ini.rt
Source: install\sample.log.properties;                  DestDir: {app}\web\WEB-INF;     Flags: onlyifdoesntexist; DestName: log.properties

Source: install\543.bat;                                DestDir: {app}\web\WEB-INF;     Flags: ignoreversion deleteafterinstall;
Source: install\rename.dirs.bat;                        DestDir: {app};                 Flags: ignoreversion deleteafterinstall;



[Run]
;Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}
;Filename: jc.bat; Parameters: repl-service-install; WorkingDir: {app}


[UninstallRun]
Filename: jc.bat; Parameters: repl-service-remove; WorkingDir: {app}



[UninstallDelete]
Name: {app}\output.err;   Type: files
Name: {app}\output.msg;   Type: files
Name: {app}\jdtx.log;     Type: files
Name: {app}\jdtx-dbm.log; Type: files
Name: {app}\jre;          Type: filesandordirs


[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=Перед началом установки рекомендуется закрыть все прочие программы.
ClickNext=Нажмите «Далее», чтобы установить версию _CFG_APP_VERSION, или «Отмена», чтобы отменить установку.

StatusRunProgram=Заключительные шаги установки...

UninstalledAll=Программа %1 была удалена с Вашего компьютера.%nИз соображений сохранности данных рабочие каталоги (в каталоге web\WEB-INF\*) не были удалены. Вы можете удалить их самостоятельно.



[Types]
Name: ClientInstall; Description: Рабочая станция
Name: ServerInstall; Description: Установка на сервер; Flags: iscustom



[Components]
Name: ClientComponent; Description: Конфигурация рабочей станции; Types: ClientInstall; Flags: fixed
Name: ServerComponent; Description: Серверная конфигурация; Types: ServerInstall; Flags: fixed



[Code]


const
  SetupMutexName = 'Jadatex.Sync';


function CheckSetupDir(): boolean;
var
  s, s_head, s_tail, setupFileName, setupFileNameBat, setupDirValid, setupParamStr: string;
  i: integer;
  resultCode: integer;
begin
  Result := False;

  //
  for i:=1 to ParamCount() do
  begin
    s:=ParamStr(i);
    s_head:=Copy(s, 1, 5);
    s_tail:=Copy(s, Length(s)-3, 4);

    //MsgBox('s_head: ' + s_head, mbError, MB_OK);
    //MsgBox('s_tail: ' + s_tail, mbError, MB_OK);

    if (s_head='/DIR=') and (s_tail='\web') then
    begin
      // Вытащим обновлялку из дистрибутива
      ExtractTemporaryFile('543.bat');
      setupFileNameBat:=ExpandConstant('{tmp}\543.bat');
      //
      setupDirValid:=Copy(s, 6, Length(s)-3-5); //setupDirValid:='C:\Users\Public\Documents\Jadatex.Sync';
      //
      setupFileName:=ExpandConstant('{srcexe}');
      //
      setupParamStr:=setupFileName+' /SILENT /repl-service-install /DIR="' + setupDirValid + '"';
      //
      if ShellExec('', setupFileNameBat, setupParamStr, '', SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode) then
      begin
        //MsgBox('Exec Ok, setupFileName: ' + setupFileNameBat + ', params: ' + setupParamStr + ', resultCode: ' + intToStr(resultCode), mbError, MB_OK);
      end
      else
      begin
        //MsgBox('Exec error, setupFileName: ' + setupFileNameBat + ', params: ' + setupParamStr + ', resultCode: ' + intToStr(resultCode), mbError, MB_OK);
      end;

      //
      exit;
    end;
  end;

  //
  Result := True;
end;


function InitializeSetup(): Boolean;
begin
  // Криво написанное авто обновление 543
  if (not CheckSetupDir()) then
  begin
    Result := False;
    exit;
  end;

  
  Result := True;
  if CheckForMutexes(SetupMutexName) then
  begin
    Log('Mutex exists, setup is running already, silently aborting');
    if (not WizardSilent) then
    begin
      MsgBox('Установка '+ExpandConstant('{#AppName}')+' уже запущена, дождитесь её завершения.', mbError, MB_OK);
    end;
    Result := False;
  end
    else
  begin
    Log('Creating mutex');
    CreateMutex(SetupMutexName);
  end;
end;


procedure CurStepChanged(CurStep: TSetupStep); 
var
  resultCode: Integer;
  i: Integer;
begin
  if (CurStep=ssInstall) then
  begin
    Exec(ExpandConstant('jc.bat'), 'repl-service-stop' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-stop.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
    Exec(ExpandConstant('jc.bat'), 'repl-service-stop' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-stop.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
    Exec(ExpandConstant('jc.bat'), 'repl-service-remove' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-remove.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
  end;


  // Перименуем папки старых рабочих каталогов
  if (CurStep=ssDone) then
  begin
    //MsgBox('run: ' + ExpandConstant('rename.dirs.bat'), mbError, MB_OK);
    Exec(ExpandConstant('rename.dirs.bat'), ' >> ' + ExpandConstant('{app}') + '\jdtx.rename.dirs.bat.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
    //MsgBox('run: ' + ExpandConstant('rename.dirs.bat') + ', params: ' + ExpandConstant('{app}'), mbError, MB_OK);
  end;


  //
  if (CurStep=ssDone) then
  begin
    for i:=1 to ParamCount() do
    begin
      if Lowercase(ParamStr(i)) = '/repl-service-install' then
      begin
        Exec(ExpandConstant('jc.bat'), 'repl-service-install' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-install.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
        Exec(ExpandConstant('jc.bat'), 'repl-service-start' + ' >> '+ ExpandConstant('{app}') + '\jdtx.repl-service-start.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
        exit;
      end;
    end;

    //MsgBox('repl-service-remove', mbInformation, MB_OK);
    Exec(ExpandConstant('jc.bat'), 'repl-service-remove' + ' >> ' + ExpandConstant('{app}') + '\jdtx.repl-service-remove.log', ExpandConstant('{app}'), SW_SHOWMINIMIZED, ewWaitUntilTerminated, resultCode);
  end;

end;

