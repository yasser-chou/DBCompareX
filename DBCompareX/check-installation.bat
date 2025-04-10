@echo off
echo Checking Angular installation...

if not exist node_modules (
    echo ERROR: node_modules directory not found!
    echo Please run install-deps.bat first.
    pause
    exit /b 1
)

if not exist node_modules\@angular (
    echo ERROR: Angular modules not found!
    echo Please run install-deps.bat first.
    pause
    exit /b 1
)

echo Checking for key Angular modules...
if exist node_modules\@angular\core (
    echo @angular/core: OK
) else (
    echo @angular/core: MISSING
)

if exist node_modules\@angular\platform-browser (
    echo @angular/platform-browser: OK
) else (
    echo @angular/platform-browser: MISSING
)

if exist node_modules\@angular\material (
    echo @angular/material: OK
) else (
    echo @angular/material: MISSING
)

echo.
echo Installation check complete!
echo.
echo To start the application, run: npm start
pause 