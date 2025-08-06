import pytest
import pandas as pd
from pathlib import Path
from datetime import date
from unittest.mock import patch, AsyncMock
from app.services.parser import ReportParser


class TestDownloader:
    @pytest.fixture
    def parser(self):
        return ReportParser()

    @pytest.mark.parametrize("is_excepted, exp_result, is_called", [
        (False, 1, True),
        (True, 0, False),
    ])
    @pytest.mark.asyncio
    async def test_save_to_database(self, parser, is_excepted, exp_result, is_called):
        df = pd.DataFrame([{
            "exchange_product_id": "TRD-ABCD123456",
            "exchange_product_name": "Продукт",
            "oil_id": "TRD-ABCD12",
            "delivery_basis_id": "ABCD123456",
            "delivery_basis_name": "Базис",
            "delivery_type_id": "23456",
            "volume": 1000.5,
            "total": 500000.0,
            "count": 3,
            "date": date(2025, 8, 1)
        }])

        mock_session = AsyncMock()
        if is_excepted:
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
        if is_called:
            mock_session.commit.assert_called_once()
        else:
            mock_session.commit.assert_not_called()

    @pytest.mark.parametrize("is_excepted, exp_result", [
        (False, 1),
        (True, 0),
    ])
    @pytest.mark.asyncio
    async def test_process_file(self, parser, is_excepted, exp_result):
        mock_semaphore = AsyncMock()
        mock_semaphore.__aenter__ = AsyncMock()
        mock_semaphore.__aexit__ = AsyncMock()

        if is_excepted:
            with patch.object(ReportParser, "parse_xls_file", side_effect=Exception("Parse error")):
                result = await parser.process_file(Path("test.xls"), mock_semaphore)
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
                    patch.object(ReportParser, "save_to_database", AsyncMock(return_value=1)):
                result = await parser.process_file(Path("oil_xls_20250804.xls"), mock_semaphore)

        assert result == exp_result
        mock_semaphore.__aenter__.assert_awaited_once()
        mock_semaphore.__aexit__.assert_awaited_once()

    @pytest.mark.parametrize("files, res, exp_result", [
        ([Path("file1.xls"), Path("file2.xls")], [1, 1], 2),
        ([], [], 0),
    ])
    @pytest.mark.asyncio
    async def test_process_directory(self, parser, files, res, exp_result):
        with patch.object(Path, "glob", return_value=files):
            if files:
                with patch.object(ReportParser, "process_file", AsyncMock(side_effect=res)) as mock_method:
                    result = await parser.process_directory(Path("test_dir"))
                    assert mock_method.await_count == len(files)
            else:
                result = await parser.process_directory(Path("test_dir"))

        assert result == exp_result
