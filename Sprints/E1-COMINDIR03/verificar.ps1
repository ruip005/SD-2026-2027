$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
foreach ($role in @('produtor', 'lider', 'replica', 'processador')) {
    $project = Join-Path $taskRoot "rep03-$role"
    $sources = @(Get-ChildItem -LiteralPath "$project/src/rep03" -Filter '*.java' | ForEach-Object FullName)
    & javac --release 11 -encoding UTF-8 -d "$project/out" @sources
    if ($LASTEXITCODE -ne 0) { throw "Compilação falhou: $role" }
}
$classPath = (@('lider', 'produtor', 'replica', 'processador') | ForEach-Object {
    Join-Path $taskRoot "rep03-$_/out"
}) -join ';'
$testOut = Join-Path $taskRoot 'tests/out'
$testSources = @(Get-ChildItem -LiteralPath "$taskRoot/tests/rep03" -Filter '*.java' | ForEach-Object FullName)
& javac --release 11 -encoding UTF-8 -cp $classPath -d $testOut @testSources
if ($LASTEXITCODE -ne 0) { throw 'Compilação dos testes falhou' }
$runFolder = Join-Path $taskRoot ("ensaios-locais/teste-" + [guid]::NewGuid().ToString('N'))
& java -cp "$testOut;$classPath" rep03.ClockIntegrationTest $runFolder
if ($LASTEXITCODE -ne 0) { throw 'Testes falharam' }
Write-Output "Ficheiros do ensaio: $runFolder"
