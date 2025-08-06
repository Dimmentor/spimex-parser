import pytest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch, MagicMock
from app.services.trading_service import TradingService


class TestTradingService:
    @pytest.fixture
    def trading_service(self):
        return TradingService()

    @pytest.fixture
    def mock_redis(self):
        return AsyncMock()

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        return session

    @pytest.fixture
    def sample_trading_result(self):
        return {
            "id": 1,
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
            "created_on": datetime(2025, 8, 4, 12, 0),
            "updated_on": datetime(2025, 8, 4, 12, 0)
        }

    @pytest.mark.asyncio
    async def test_get_last_trading_dates_from_cache(self, trading_service, mock_redis):
        test_dates = ["2025-08-04", "2025-08-03"]
        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache',
                      return_value={"dates": test_dates}):
            result = await trading_service.get_last_trading_dates()
            assert result == [date(2025, 8, 4), date(2025, 8, 3)]

    @pytest.mark.asyncio
    async def test_get_last_trading_dates_from_db(self, trading_service, mock_redis, mock_session):
        test_dates = [date(2025, 8, 4), date(2025, 8, 3)]

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(date(2025, 8, 4),), (date(2025, 8, 3),)]

        mock_session.execute.return_value = mock_result

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=None), \
                patch('app.services.trading_service.AsyncSessionLocal', return_value=mock_session), \
                patch('app.services.trading_service.TradingService._set_cache') as mock_set_cache:
            result = await trading_service.get_last_trading_dates()

            assert result == test_dates
            mock_session.execute.assert_called_once()
            mock_set_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_dynamics_from_cache(self, trading_service, mock_redis):
        test_data = {
            "results": [{
                "id": 1,
                "exchange_product_id": "A100NVY060F",
                "date": "2025-08-04"
            }]
        }

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=test_data):
            result = await trading_service.get_dynamics(
                start_date=date(2025, 8, 1),
                end_date=date(2025, 8, 4))

            assert result == test_data["results"]

    @pytest.mark.asyncio
    async def test_get_dynamics_from_db(self, trading_service, mock_redis, mock_session, sample_trading_result):
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.fetchall.return_value = [MagicMock(**sample_trading_result)]
        mock_result.scalars.return_value = mock_scalars

        mock_session.execute.return_value = mock_result

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=None), \
                patch('app.services.trading_service.AsyncSessionLocal', return_value=mock_session), \
                patch('app.services.trading_service.TradingService._set_cache') as mock_set_cache:
            result = await trading_service.get_dynamics(
                start_date=date(2025, 8, 1),
                end_date=date(2025, 8, 4),
                oil_id="A100NVY06")

            assert len(result) == 1
            assert result[0]["id"] == sample_trading_result["id"]
            mock_session.execute.assert_called_once()
            mock_set_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_trading_results_from_cache(self, trading_service, mock_redis):
        test_data = {
            "results": [{
                "id": 1,
                "oil_id": "A100NVY06",
                "date": "2025-08-04"
            }]
        }

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=test_data):
            result = await trading_service.get_trading_results(oil_id="A100NVY06")

            assert result == test_data["results"]

    @pytest.mark.asyncio
    async def test_get_trading_results_from_db(self, trading_service, mock_redis, mock_session, sample_trading_result):
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.fetchall.return_value = [MagicMock(**sample_trading_result)]
        mock_result.scalars.return_value = mock_scalars

        mock_session.execute.return_value = mock_result

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=None), \
                patch('app.services.trading_service.AsyncSessionLocal', return_value=mock_session), \
                patch('app.services.trading_service.TradingService._set_cache') as mock_set_cache:
            result = await trading_service.get_trading_results(
                oil_id="A100NVY06",
                delivery_type_id="060F",
                limit=10)

            assert len(result) == 1
            assert result[0]["oil_id"] == "A100NVY06"
            mock_session.execute.assert_called_once()
            mock_set_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_trading_results_with_filters(self, trading_service, mock_redis, mock_session):
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.fetchall.return_value = []
        mock_result.scalars.return_value = mock_scalars

        mock_session.execute.return_value = mock_result

        with patch('app.services.trading_service.TradingService._get_redis', return_value=mock_redis), \
                patch('app.services.trading_service.TradingService._get_from_cache', return_value=None), \
                patch('app.services.trading_service.AsyncSessionLocal', return_value=mock_session):
            await trading_service.get_trading_results(
                oil_id="A100NVY06",
                delivery_type_id="060F",
                delivery_basis_id="NVY060F")

            called_query = mock_session.execute.call_args[0][0]
            assert "oil_id = :oil_id_1" in str(called_query)
            assert "delivery_type_id = :delivery_type_id_1" in str(called_query)
            assert "delivery_basis_id = :delivery_basis_id_1" in str(called_query)