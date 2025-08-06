import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient
from datetime import date
from app.main import app

client = TestClient(app)


class TestEndpoints:
    @pytest.mark.parametrize(
        "params, mocked_report, exp_status, exp_response",
        [
            (
                    {},
                    ["file1.xls", "file2.xls"],
                    200,
                    ["file1.xls", "file2.xls"]
            ),
            (
                    {"start_date": "2023-01-01", "end_date": "2023-01-10"},
                    ["file1.xls"],
                    200,
                    ["file1.xls"]
            ),
            (
                    {},
                    Exception("Download failed"),
                    500,
                    {"detail": "Download failed"}
            ),
        ]
    )
    @patch('app.services.downloader.ReportDownloader.get_and_save_reports')
    def test_download_reports(self, mock_download, params, mocked_report, exp_status, exp_response):
        if isinstance(mocked_report, Exception):
            mock_download.side_effect = mocked_report
        else:
            mock_download.return_value = mocked_report

        response = client.post("/download-reports/", params=params)
        assert response.status_code == exp_status
        assert response.json() == exp_response

    @pytest.mark.parametrize(
        "mocked_report, exp_status, exp_response",
        [
            (
                    5,
                    200,
                    {"message": "Отчёты успешно обработаны", "records_processed": 5}
            ),
            (
                    Exception("Processing failed"),
                    500,
                    {"detail": "Processing failed"}
            ),
        ]
    )
    @patch('app.services.parser.ReportParser.process_directory')
    def test_process_reports(self, mock_process, mocked_report, exp_status, exp_response):
        if isinstance(mocked_report, Exception):
            mock_process.side_effect = mocked_report
        else:
            mock_process.return_value = mocked_report

        response = client.post("/process-reports/")
        assert response.status_code == exp_status
        assert response.json() == exp_response

    @pytest.mark.parametrize(
        "params, mocked_dates, exp_status, exp_response",
        [
            (
                    {},
                    [date(2023, 1, 1), date(2023, 1, 2)],
                    200,
                    {"dates": ["2023-01-01", "2023-01-02"], "count": 2}
            ),
            (
                    {"limit": 20},
                    [date(2023, 1, 1)],
                    200,
                    {"dates": ["2023-01-01"], "count": 1}
            ),
            (
                    {"limit": 101},
                    [date(2023, 1, 1)],
                    422,
                    None
            ),
            (
                    {},
                    Exception("Database error"),
                    500,
                    {"detail": "Database error"}
            ),
        ]
    )
    @patch('app.services.trading_service.TradingService.get_last_trading_dates')
    def test_get_last_trading_dates(self, mock_trading, params, mocked_dates, exp_status, exp_response):
        if isinstance(mocked_dates, Exception):
            mock_trading.side_effect = mocked_dates
        else:
            mock_trading.return_value = mocked_dates

        response = client.get("/trading/last-dates/", params=params)
        assert response.status_code == exp_status
        if exp_response:
            assert response.json() == exp_response

    @pytest.mark.parametrize(
        "request_data, mocked_response, exp_status, exp_response",
        [
            (
                    {
                        "start_date": "2025-07-11",
                        "end_date": "2025-07-11",
                        "oil_id": "TRD-RFF060"
                    },
                    [
                        {
                            "id": 254820,
                            "exchange_product_id": "TRD-RFF060C",
                            "exchange_product_name": "Топливо для реактивных двигателей",
                            "oil_id": "TRD-RFF060",
                            "delivery_basis_id": "RFF060C",
                            "delivery_basis_name": "РФ БП",
                            "delivery_type_id": "F060C",
                            "volume": 1500,
                            "total": 119221500,
                            "count": 1,
                            "date": "2025-07-11",
                            "created_on": "2025-07-23T15:42:19.760432",
                            "updated_on": "2025-07-23T15:42:19.760432"
                        }
                    ],
                    200,
                    {"count": 1, "results": [{"oil_id": "TRD-RFF060"}]}
            ),
            (
                    {
                        "start_date": "2025-07-12",
                        "end_date": "2025-07-11",
                        "oil_id": "TRD-RFF060"
                    },
                    None,
                    400,
                    {"detail": "Начальная дата не может быть позже конечной даты"}
            ),
            (
                    {
                        "start_date": "2025-07-11",
                        "end_date": "2025-07-11",
                        "oil_id": "TRD-RFF060"
                    },
                    Exception("Service error"),
                    500,
                    {"detail": "Service error"}
            ),
        ]
    )
    @patch('app.api.endpoints.TradingService')
    def test_get_dynamics(self, mock_trading_service, request_data, mocked_response, exp_status, exp_response):
        mock_service_instance = mock_trading_service.return_value
        mock_service_instance.close = AsyncMock()

        if isinstance(mocked_response, Exception):
            mock_service_instance.get_dynamics = AsyncMock(side_effect=mocked_response)
        else:
            mock_service_instance.get_dynamics = AsyncMock(return_value=mocked_response)

        response = client.post("/trading/dynamics/", json=request_data)
        assert response.status_code == exp_status

        if exp_status == 200:
            assert response.json()["count"] == exp_response["count"]
            assert response.json()["results"][0]["oil_id"] == exp_response["results"][0]["oil_id"]
        elif exp_status >= 400:
            assert response.json() == exp_response

    @pytest.mark.parametrize(
        "request_data, mocked_response, exp_status, exp_response",
        [
            (
                    {"oil_id": "TRD-RFF060", "limit": 10},
                    [
                        {
                            "id": 254820,
                            "exchange_product_id": "TRD-RFF060C",
                            "exchange_product_name": "Топливо для реактивных двигателей",
                            "oil_id": "TRD-RFF060",
                            "delivery_basis_id": "RFF060C",
                            "delivery_basis_name": "РФ БП",
                            "delivery_type_id": "F060C",
                            "volume": 1500,
                            "total": 119221500,
                            "count": 1,
                            "date": "2025-07-11",
                            "created_on": "2025-07-23T15:42:19.760432",
                            "updated_on": "2025-07-23T15:42:19.760432"
                        }
                    ],
                    200,
                    {"count": 1, "results": [{"oil_id": "TRD-RFF060"}]}
            ),

            (
                    {"oil_id": "TRD-RFF060", "limit": 1001},
                    None,
                    422,
                    None
            ),
            (
                    {"oil_id": "TRD-RFF060"},
                    Exception("Service error"),
                    500,
                    {"detail": "Service error"}
            ),
        ]
    )
    @patch('app.api.endpoints.TradingService')
    def test_get_trading_results(self, mock_trading_service, request_data, mocked_response, exp_status,
                                 exp_response):
        mock_service_instance = mock_trading_service.return_value
        mock_service_instance.close = AsyncMock()

        if isinstance(mocked_response, Exception):
            mock_service_instance.get_trading_results = AsyncMock(side_effect=mocked_response)
        else:
            mock_service_instance.get_trading_results = AsyncMock(return_value=mocked_response)

        response = client.post("/trading/results/", json=request_data)
        assert response.status_code == exp_status

        if exp_status == 200:
            assert response.json()["count"] == exp_response["count"]
            assert response.json()["results"][0]["oil_id"] == exp_response["results"][0]["oil_id"]
        elif exp_status >= 400 and exp_response:
            assert response.json() == exp_response
