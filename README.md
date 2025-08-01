<h2>Асинхронный парсер на FastAPI</h2>

Основной стек: FastAPI(Python), PostgreSQL, Redis.

UPD: Сервис переделан под асинхронность: использован асинхронный движок, асинхронные миграции, функции скачивания
и парсинга переделаны под асинхронность. Выставлено ограничение с помощью semaphore.

PS: Итоговые метрики обновлены.

* Скачивает бюллетени по итогам торгов с сайта биржи (https://spimex.com/markets/oil_products/trades/results/)

* Записывает данные из отчётов в БД

* Проводит анализ торгов, кэширует в Redis, отдает в JSON-формате

<h3>Краткий гайд по установке:</h3>

1) Клонировать репозиторий

```sh
  git clone https://github.com/Dimmentor/spimex-parser.git
```

2) Сборка и запуск всех сервисов(миграция применится автоматически)

```sh
  docker-compose up --build
```


<h3>Для удобства реализованы эндпоинты, можно использовать Swagger(http://127.0.0.1:8000/docs) или Postman</h3>

1) Запуск скачивания отчётов(по умолчанию с 01.01.2023 по текущую дату)

```sh
POST /download-reports/
```

* Либо выбрать дату вручную

```sh
POST /download-reports/?start_date=2023-01-01&end_date=2025-07-11
```

2) Запуск обработки всех скачанных файлов и записи в БД

```sh
POST /process-reports/
```

<h3>Далее эндпоинты в рамках практики по FastAPI</h3>

3) Список дат последних торговых дней

По умолчанию выдает последние 10 торговых дней(последние 10 дат, существующие в БД. Лимит - 100 дней)

```sh
GET /trading/last-dates/
```

или

```sh
GET /trading/last-dates/?limit=20
```

Пример ответа:

```sh
{
  "dates": [
    "2025-07-11",
    "2025-07-10",
    "2025-07-09",
    "2025-07-08",
    "2025-07-07",
    "2025-07-04",
    "2025-07-03",
    "2025-07-02",
    "2025-07-01",
    "2025-06-30"
  ],
  "count": 10
}
```

4) Список торгов за заданный период(фильтрация по oil_id, delivery_type_id, delivery_basis_id, start_date, end_date)

```sh
POST /trading/dynamics/
```

Пример тела запроса(обязательными полями являются даты и один из id):

```sh
{
  "start_date": "2025-07-01",
  "end_date": "2025-07-11",
  "oil_id": "TRD-RFF060"
}
```

Пример ответа:

```sh
{
  "results": [
    {
      "id": 254820,
      "exchange_product_id": "TRD-RFF060C",
      "exchange_product_name": "Топливо для реактивных двигателей марок РТ в/с ТС-1 в/с, РФ БП (ст. назначения)",
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
    },
    {
      "id": 254121,
      "exchange_product_id": "TRD-RFF060C",
      "exchange_product_name": "Топливо для реактивных двигателей марок РТ в/с ТС-1 в/с, РФ БП (ст. назначения)",
      "oil_id": "TRD-RFF060",
      "delivery_basis_id": "RFF060C",
      "delivery_basis_name": "РФ БП",
      "delivery_type_id": "F060C",
      "volume": 1500,
      "total": 119155500,
      "count": 1,
      "date": "2025-07-07",
      "created_on": "2025-07-23T15:42:18.256320",
      "updated_on": "2025-07-23T15:42:18.256320"
    }
  ],
  "count": 2
}
```

5) Список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)

```sh
POST /trading/results/
```

Пример тела запроса(обязательными полями являются даты и один из id):

```sh
{
  "start_date": "2025-07-01",
  "end_date": "2025-07-11",
  "oil_id": "TRD-RFF060"
}
```


Пример ответа:
```sh
{
  "results": [
    {
      "id": 254820,
      "exchange_product_id": "TRD-RFF060C",
      "exchange_product_name": "Топливо для реактивных двигателей марок РТ в/с ТС-1 в/с, РФ БП (ст. назначения)",
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
    },
    ......
```

<h3>Итоговые метрики парсера:</h3>

* 127410 записей с 01.01.2023 по 11.07.2025(621 отчёт)

* Скачивание всех отчётов при синхронном способе выполняется за ~25 минут
* UPD: при асинхронном способе скачивание выполняется за ~3 минуты (при выставленном ограничении 10
  скачиваний/одновременно)

* Обработка и загрузка данных в БД при синхронном способе за ~4 минуты
* UPD: при асинхронном способе парсинг выполняется за ~3 минуты (при выставленном ограничении 10 парсингов/одновременно)

Ограничения снимать не стал. Но, думаю, эти операции можно выполнить ещё быстрее, главное не нарваться на блокировку.
