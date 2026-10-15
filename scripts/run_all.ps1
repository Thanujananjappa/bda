# End-to-end: dataset -> train -> (optional Kafka/HDFS) -> score -> build UI
param(
    [switch]$WithDocker,
    [switch]$SkipProducer,
    [switch]$SkipFrontend,
    [int]$ProducerWeeks = 4,
    [int]$ProducerStudents = 200
)

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

Write-Host "==> Installing Python dependencies" -ForegroundColor Cyan
python -m pip install -r requirements.txt -q

Write-Host "==> Download UCI dataset" -ForegroundColor Cyan
python scripts/download_dataset.py

if ($WithDocker) {
    Write-Host "==> Starting Kafka + HDFS (Docker)" -ForegroundColor Cyan
    docker compose up -d
    Start-Sleep -Seconds 25
    docker compose run --rm kafka-init
    docker compose run --rm hdfs-init
}

if (-not $SkipProducer) {
    if ($WithDocker) {
        Write-Host "==> Kafka producer (sample stream)" -ForegroundColor Cyan
        python pipeline/kafka_producer.py --weeks $ProducerWeeks --limit-students $ProducerStudents --delay 0
        Write-Host "==> Kafka -> HDFS consumer" -ForegroundColor Cyan
        python pipeline/kafka_to_hdfs_consumer.py --max-messages 50000
    } else {
        Write-Host "==> Skipping Kafka (no -WithDocker). Scoring uses UCI stream simulation." -ForegroundColor Yellow
    }
}

Write-Host "==> Train classifier" -ForegroundColor Cyan
python ml/train_model.py

Write-Host "==> Weekly dropout risk scores" -ForegroundColor Cyan
python ml/weekly_scoring.py

if (-not $SkipFrontend) {
    Write-Host "==> Build React advisor UI" -ForegroundColor Cyan
    & (Join-Path $Root "scripts\build_frontend.ps1")
}

Write-Host "==> Start platform: python dashboard/app.py" -ForegroundColor Green
Write-Host "    Open http://localhost:8080 (API + React UI)" -ForegroundColor Green
Write-Host "    Dev UI only: cd frontend && npm run dev  (proxies /api to :8080)" -ForegroundColor Green
