import json
from datetime import date, datetime, time
from typing import List, Optional, Dict, Any
from sqlalchemy import select, desc, and_
import redis.asyncio as redis
from ..models import SpimexTradingResult
from ..database import AsyncSessionLocal
from ..config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
from ..utils.logger import logger


class TradingService:
    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self.cache_ttl = 3600
        self.cache_reset_time = time(14, 11)  # без привязки в часовому поясу

    async def _get_redis(self) -> redis.Redis:
        if self.redis is None:
            self.redis = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
        try:
            await self.redis.ping()
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            self.redis = None
            raise
        return self.redis

    async def _get_cache_key(self, method: str, **params) -> str:
        param_str = "_".join([f"{k}_{v}" for k, v in sorted(params.items())])
        return f"trading:{method}:{param_str}"

    async def _should_reset_cache(self) -> bool:
        now = datetime.now().time()
        return now >= self.cache_reset_time

    async def _clear_cache_if_needed(self):
        if await self._should_reset_cache():
            redis_client = await self._get_redis()
            pattern = "trading:*"
            keys = await redis_client.keys(pattern)
            if keys:
                await redis_client.delete(*keys)
                logger.info(f"Кэш очищен: удалено {len(keys)} ключей")

    async def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        try:
            redis_client = await self._get_redis()
            data = await redis_client.get(cache_key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Ошибка получения из кэша: {e}")
        return None

    async def _set_cache(self, cache_key: str, data: Dict[str, Any]):
        try:
            redis_client = await self._get_redis()
            await redis_client.setex(cache_key, self.cache_ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Ошибка сохранения в кэш: {e}")

    async def get_last_trading_dates(self, limit: int = 10) -> List[date]:
        await self._clear_cache_if_needed()
        cache_key = await self._get_cache_key("last_trading_dates", limit=limit)
        cached_data = await self._get_from_cache(cache_key)

        if cached_data:
            return [date.fromisoformat(d) for d in cached_data["dates"]]

        async with AsyncSessionLocal() as session:
            query = select(SpimexTradingResult.date) \
                .distinct() \
                .order_by(desc(SpimexTradingResult.date)) \
                .limit(limit)

            result = await session.execute(query)
            dates = [row[0] for row in result.fetchall()]

            cache_data = {"dates": [d.isoformat() for d in dates]}
            await self._set_cache(cache_key, cache_data)

            return dates

    async def get_dynamics(
            self,
            start_date: date,
            end_date: date,
            oil_id: Optional[str] = None,
            delivery_type_id: Optional[str] = None,
            delivery_basis_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:

        await self._clear_cache_if_needed()

        cache_key = await self._get_cache_key(
            "dynamics",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            oil_id=oil_id or "",
            delivery_type_id=delivery_type_id or "",
            delivery_basis_id=delivery_basis_id or ""
        )

        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            return cached_data["results"]

        async with AsyncSessionLocal() as session:
            conditions = [
                SpimexTradingResult.date >= start_date,
                SpimexTradingResult.date <= end_date
            ]

            if oil_id:
                conditions.append(SpimexTradingResult.oil_id == oil_id)
            if delivery_type_id:
                conditions.append(SpimexTradingResult.delivery_type_id == delivery_type_id)
            if delivery_basis_id:
                conditions.append(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

            query = select(SpimexTradingResult) \
                .where(and_(*conditions)) \
                .order_by(SpimexTradingResult.date.desc(), SpimexTradingResult.id)

            result = await session.execute(query)
            records = result.scalars().fetchall()

            results = []
            for record in records:
                results.append({
                    "id": record.id,
                    "exchange_product_id": record.exchange_product_id,
                    "exchange_product_name": record.exchange_product_name,
                    "oil_id": record.oil_id,
                    "delivery_basis_id": record.delivery_basis_id,
                    "delivery_basis_name": record.delivery_basis_name,
                    "delivery_type_id": record.delivery_type_id,
                    "volume": float(record.volume) if record.volume else None,
                    "total": float(record.total) if record.total else None,
                    "count": record.count,
                    "date": record.date.isoformat(),
                    "created_on": record.created_on.isoformat() if record.created_on else None,
                    "updated_on": record.updated_on.isoformat() if record.updated_on else None
                })

            cache_data = {"results": results}
            await self._set_cache(cache_key, cache_data)

            return results

    async def get_trading_results(
            self,
            oil_id: Optional[str] = None,
            delivery_type_id: Optional[str] = None,
            delivery_basis_id: Optional[str] = None,
            limit: int = 100
    ) -> List[Dict[str, Any]]:

        await self._clear_cache_if_needed()

        cache_key = await self._get_cache_key(
            "trading_results",
            oil_id=oil_id or "",
            delivery_type_id=delivery_type_id or "",
            delivery_basis_id=delivery_basis_id or "",
            limit=limit
        )

        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            return cached_data["results"]

        async with AsyncSessionLocal() as session:
            conditions = []

            if oil_id:
                conditions.append(SpimexTradingResult.oil_id == oil_id)
            if delivery_type_id:
                conditions.append(SpimexTradingResult.delivery_type_id == delivery_type_id)
            if delivery_basis_id:
                conditions.append(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

            query = select(SpimexTradingResult)
            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(desc(SpimexTradingResult.date), desc(SpimexTradingResult.id)).limit(limit)

            result = await session.execute(query)
            records = result.scalars().fetchall()

            results = []
            for record in records:
                results.append({
                    "id": record.id,
                    "exchange_product_id": record.exchange_product_id,
                    "exchange_product_name": record.exchange_product_name,
                    "oil_id": record.oil_id,
                    "delivery_basis_id": record.delivery_basis_id,
                    "delivery_basis_name": record.delivery_basis_name,
                    "delivery_type_id": record.delivery_type_id,
                    "volume": float(record.volume) if record.volume else None,
                    "total": float(record.total) if record.total else None,
                    "count": record.count,
                    "date": record.date.isoformat(),
                    "created_on": record.created_on.isoformat() if record.created_on else None,
                    "updated_on": record.updated_on.isoformat() if record.updated_on else None
                })

            cache_data = {"results": results}
            await self._set_cache(cache_key, cache_data)

            return results

    async def close(self):
        if self.redis:
            await self.redis.close()
            self.redis = None
