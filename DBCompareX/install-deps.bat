@echo off
echo Cleaning npm cache...
npm cache clean --force
echo.

echo Removing existing node_modules and package-lock.json...
if exist node_modules rmdir /s /q node_modules
if exist package-lock.json del package-lock.json
echo.

echo Installing Angular dependencies...
npm install --legacy-peer-deps
echo.

echo Installation complete!
echo.
echo To start the application, run: npm start
pause 