import pytest
import pandas as pd
from pathlib import Path
from datetime import date
from unittest.mock import patch, AsyncMock
from app.services.parser import ReportParser


class TestParser:
    @pytest.fixture
    def parser(self):
        return ReportParser()

    @pytest.mark.parametrize(
        "raise_error, exp_result, commit_called",
        [
            (False, 1, True),
            (True, 0, False),
        ]
    )
    @pytest.mark.asyncio
    async def test_save_to_database(self, parser, raise_error, exp_result, commit_called):
        df = pd.DataFrame([{
            "exchange_product_id": "A100NVY060F",
            "exchange_product_name": "Бензин (АИ-100-К5)",
            "oil_id": "A100NVY06",
            "delivery_basis_id": "NVY060F",
            "delivery_basis_name": "ст. Новоярославская",
            "delivery_type_id": "060F",
            "volume": 100.0,
            "total": 500000.0,
            "count": 1,
            "date": date(2025, 8, 4),
        }])

        mock_session = AsyncMock()
        if raise_error:
            mock_session.run_sync.side_effect = Exception()
        else:
            mock_session.run_sync.return_value = None
            mock_session.commit.return_value = None

        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_session
        mock_context_manager.__aexit__.return_value = None

        with patch("app.services.parser.AsyncSessionLocal", return_value=mock_context_manager):
            result = await parser.save_to_database(df)

        assert result == exp_result
        mock_session.run_sync.assert_called_once()
        if commit_called:
            mock_session.commit.assert_called_once()
        else:
            mock_session.commit.assert_not_called()

    @pytest.mark.parametrize("test_case", [
        {
            "name": "success_case",
            "parse_raises": False,
            "filename": "oil_xls_20250804.xls",
            "exp_result": 1,
            "save_result": 1
        },
        {
            "name": "error_case",
            "parse_raises": True,
            "filename": "invalid_file.xls",
            "exp_result": 0,
            "save_result": None
        }
    ])
    @pytest.mark.asyncio
    async def test_process_file(self, parser, test_case):
        mock_semaphore = AsyncMock()
        mock_semaphore.__aenter__ = AsyncMock()
        mock_semaphore.__aexit__ = AsyncMock()

        if test_case["parse_raises"]:
            with patch.object(ReportParser, "parse_xls_file", side_effect=Exception("Error")):
                result = await parser.process_file(Path(test_case["filename"]), mock_semaphore)
        else:
            test_data = {
                "exchange_product_id": ["A100NVY060F"],
                "exchange_product_name": ["Бензин (АИ-100-К5)"],
                "oil_id": ["A100NVY06"],
                "delivery_basis_id": ["NVY060F"],
                "delivery_basis_name": ["ст. Новоярославская"],
                "delivery_type_id": ["060F"],
                "volume": [100.0],
                "total": [500000.0],
                "count": [1],
                "date": [date(2025, 8, 4)],
            }
            df = pd.DataFrame(test_data)

            with patch.object(ReportParser, "parse_xls_file", return_value=df), \
                    patch.object(ReportParser, "save_to_database", AsyncMock(return_value=test_case["save_result"])):
                result = await parser.process_file(Path(test_case["filename"]), mock_semaphore)

        assert result == test_case["exp_result"]
        mock_semaphore.__aenter__.assert_awaited_once()
        mock_semaphore.__aexit__.assert_awaited_once()

    @pytest.mark.parametrize("test_case", [
        {
            "name": "success_case",
            "files": [Path("file1.xls"), Path("file2.xls")],
            "res": [1, 1],
            "exp_result": 2
        },
        {
            "name": "empty_case",
            "files": [],
            "res": [],
            "exp_result": 0
        }
    ])
    @pytest.mark.asyncio
    async def test_process_directory(self, parser, test_case):
        with patch.object(Path, "glob", return_value=test_case["files"]):
            if test_case["files"]:
                with patch.object(ReportParser, "process_file",
                                  AsyncMock(side_effect=test_case["res"])) as mock_method:
                    result = await parser.process_directory(Path("test_dir"))
                    assert mock_method.await_count == len(test_case["files"])
            else:
                result = await parser.process_directory(Path("test_dir"))

        assert result == test_case["exp_result"]
