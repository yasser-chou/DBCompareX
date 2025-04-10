Write-Host "Cleaning npm cache..." -ForegroundColor Yellow
npm cache clean --force
Write-Host ""

Write-Host "Removing existing node_modules and package-lock.json..." -ForegroundColor Yellow
if (Test-Path -Path "node_modules") {
    Remove-Item -Path "node_modules" -Recurse -Force
}
if (Test-Path -Path "package-lock.json") {
    Remove-Item -Path "package-lock.json" -Force
}
Write-Host ""

Write-Host "Installing Angular dependencies..." -ForegroundColor Green
npm install --legacy-peer-deps
Write-Host ""

Write-Host "Installation complete!" -ForegroundColor Green
Write-Host ""
Write-Host "To start the application, run: npm start" -ForegroundColor Cyan
Write-Host "Press any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") 