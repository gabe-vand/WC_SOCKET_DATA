@echo off
REM Move into the directory where this script lives
pushd "%~dp0%"

echo Compiling…
javac Utils.java Socket.java runSocket.java
if errorlevel 1 (
  echo Compilation failed.
  popd
  exit /b 1
)

echo Running runSocket…
java runSocket %*

REM Return to original directory
popd
