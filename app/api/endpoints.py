from fastapi import APIRouter, HTTPException, Query
from datetime import date
from pathlib import Path
from typing import List
from ..config import REPORTS_DIR
from ..services.downloader import ReportDownloader
from ..services.parser import ReportParser
from ..services.trading_service import TradingService
from ..schemas import (
    LastTradingDatesResponse,
    DynamicsRequest,
    DynamicsResponse,
    TradingResultsRequest,
    TradingResultsResponse
)

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


# далее эндпоинты в рамках практики по FastAPI
@router.get("/trading/last-dates/", response_model=LastTradingDatesResponse)
async def get_last_trading_dates(
        limit: int = Query(10, ge=1, le=100,
                           description="Количество последних торговых дней")) -> LastTradingDatesResponse:
    try:
        trading_service = TradingService()
        dates = await trading_service.get_last_trading_dates(limit)
        await trading_service.close()

        return LastTradingDatesResponse(
            dates=[d.isoformat() for d in dates],
            count=len(dates)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trading/dynamics/", response_model=DynamicsResponse)
async def get_dynamics(request: DynamicsRequest) -> DynamicsResponse:
    try:
        if request.start_date > request.end_date:
            raise HTTPException(status_code=400, detail="Начальная дата не может быть позже конечной даты")
        trading_service = TradingService()
        results = await trading_service.get_dynamics(
            start_date=request.start_date,
            end_date=request.end_date,
            oil_id=request.oil_id,
            delivery_type_id=request.delivery_type_id,
            delivery_basis_id=request.delivery_basis_id
        )
        await trading_service.close()

        return DynamicsResponse(
            results=results,
            count=len(results)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trading/results/", response_model=TradingResultsResponse)
async def get_trading_results(request: TradingResultsRequest) -> TradingResultsResponse:
    try:
        trading_service = TradingService()
        results = await trading_service.get_trading_results(
            oil_id=request.oil_id,
            delivery_type_id=request.delivery_type_id,
            delivery_basis_id=request.delivery_basis_id,
            limit=request.limit
        )
        await trading_service.close()

        return TradingResultsResponse(
            results=results,
            count=len(results)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
