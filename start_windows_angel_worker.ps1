$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Update these before first use.
$env:NGROK_DOMAIN = "windows-worker.ngrok.app"
$env:DISABLE_NGROK = "false"
$env:ANGEL_EXECUTOR_ONLY = "true"
$env:ANGEL_EXECUTOR_TOKEN = "replace-with-shared-secret"
$env:ANGEL_EXECUTOR_PATH = "/internal/angel-execute"

$env:ANGEL_WEB_PROFILE_DIR = "C:\Users\Administrator\Documents\Work\Code\angel_web_bot_profile_clean"
$env:ANGEL_WEB_DEBUGGER_ADDRESS = "127.0.0.1:9222"
$env:ANGEL_WEB_CHROME_BINARY = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$env:ANGEL_WEB_LOGIN_CREDENTIALS_PATH = "C:\Users\Administrator\Documents\Work\Code\angel_web_login_credentials.txt"
$env:ANGEL_WEB_LOGIN_OTP_PATH = "C:\Users\Administrator\Documents\Work\Code\angel_web_login_otp.txt"
$env:ANGEL_WEB_INSTRUMENT_FILE = "C:\Users\Administrator\Documents\Work\Code\AngelInstrumentDetails.csv"
$env:ANGEL_WEB_ATTACH_ONLY = "false"

if ($env:NGROK_DOMAIN -eq "windows-worker.ngrok.app") {
    throw "Update NGROK_DOMAIN in start_windows_angel_worker.ps1 before running."
}

if ($env:ANGEL_EXECUTOR_TOKEN -eq "replace-with-shared-secret") {
    throw "Update ANGEL_EXECUTOR_TOKEN in start_windows_angel_worker.ps1 before running."
}

$ActivateScript = Join-Path $ScriptDir ".venv\Scripts\Activate.ps1"
if (-not (Test-Path $ActivateScript)) {
    throw "Missing virtualenv activate script at $ActivateScript"
}

. $ActivateScript
python .\Server_Start.py
