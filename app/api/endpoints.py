from fastapi import APIRouter, HTTPException
from datetime import date
from pathlib import Path
from typing import List
from ..services.downloader import ReportDownloader
from ..services.parser import ReportParser
from ..config import REPORTS_DIR

router = APIRouter()


@router.post("/download-reports/")
async def download_reports(
    start_date: date = date(2023, 1, 1),
    end_date: date = date.today()
) -> List[str]:
    try:
        downloader = ReportDownloader()
        saved_files = await downloader.get_and_save_reports(start_date, end_date)
        return saved_files
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process-reports/")
async def process_reports() -> dict:
    try:
        parser = ReportParser()
        report_dir = Path(REPORTS_DIR)
        count = await parser.process_directory(report_dir)
        return {
            "message": "Отчёты успешно обработаны",
            "records_processed": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))