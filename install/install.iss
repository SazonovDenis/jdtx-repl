#define App "JadatexSync"
#define DefaultDir "JadatexSync"
#define AppName "Jadatex. Служба синхронизации"


#define AppVersion "_CFG_APP_VERSION"


[Setup]
AppID=Jadatex.Sync
AppPublisher=Компания Jadatex
AppPublisherURL=http://jadatex.com
AppSupportURL=http://jadatex.com/wiki/contacts
AppUpdatesURL=http://jadatex.com/wiki/downloads_list

OutputBaseFilename={#App}-{#AppVersion}
AppName={#AppName}
AppVerName={#App} (Версия {#AppVersion})
DefaultDirName={pf}\{#DefaultDir}
DefaultGroupName=Jadatex PawnShop

OutputDir=Z:\jdtx-repl\install
SourceDir=Z:\jdtx-repl\

;WizardImageFile=Images\PawnshopApp_install.bmp
;WizardSmallImageFile=Images\PawnshopApp_install_48.bmp
WizardImageStretch=false

DisableReadyPage=true
DisableStartupPrompt=true
UninstallRestartComputer=false
DisableProgramGroupPage=true
DisableDirPage=true
DisableFinishedPage=true
AlwaysShowComponentsList=true

Compression=lzma/ultra
SolidCompression=true
InternalCompressLevel=ultra
;SetupIconFile=Images\PawnshopApp_install_icon.ico


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
Source: temp\distr\*.*; DestDir: {app}\jc; Flags: ignoreversion recursesubdirs createallsubdirs
Source: C:\jdk1.8.0_45\jre\*.*; DestDir: {app}\jre; Flags: ignoreversion recursesubdirs createallsubdirs


[Run]
Filename: SCHTASKS; Parameters: /Delete /TN JadatexSync /f; WorkingDir: {app}
Filename: SCHTASKS; Parameters: /Create /TN JadatexSync /TR {app}\jc\run.bat /SC MINUTE /MO 5; WorkingDir: {app}

[Messages]
WelcomeLabel1=[name].
WelcomeLabel2=Перед началом установки рекомендуется закрыть все прочие программы.
ClickNext=Нажмите «Далее», чтобы установить версию _CFG_APP_VERSION, или «Отмена», чтобы отменить установку.

StatusRunProgram=Заключительные шаги установки...

UninstalledAll=Программа %1 была удалена с Вашего компьютера.%nИз соображений сохранности данных файлы рабочие каталоги (web\WEB-INF\*) не были удалены. Вы можете удалить их самостоятельно.


