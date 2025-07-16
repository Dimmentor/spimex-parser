import pandas as pd
from datetime import date
from typing import Optional
from pathlib import Path
from ..models import SpimexTradingResult
from ..database import SessionLocal
from ..utils.logger import logger


class ReportParser:
    def __init__(self):
        self.required_columns = [
            "Код Инструмента",
            "Наименование Инструмента",
            "Базис поставки",
            "Объем Договоров в единицах измерения",
            "Обьем Договоров, руб.",
            "Количество Договоров, шт."
        ]

    def _clean_column_name(self, name: str) -> str:
        if not isinstance(name, str):
            return ""
        return name.replace("\n", " ").replace("\xa0", " ").strip()

    def parse_xls_file(self, file_path: Path, report_date: date) -> Optional[pd.DataFrame]:
        try:
            df_raw = pd.read_excel(file_path, sheet_name="TRADE_SUMMARY", header=None, engine="xlrd")

            start_idx = df_raw[
                df_raw.apply(
                    lambda row: row.astype(str)
                    .str.contains("Единица измерения: Метрическая тонна")
                    .any(),
                    axis=1,
                )
            ].index[0]

            end_candidates = df_raw[
                df_raw.apply(lambda row: row.astype(str).str.contains("Итого:").any(), axis=1)
            ].index
            end_idx = end_candidates[end_candidates > start_idx].min()

            headers = df_raw.iloc[start_idx + 1].apply(self._clean_column_name).tolist()
            df_data = df_raw.iloc[start_idx + 2: end_idx].copy()
            df_data.columns = headers

            df_data = df_data[self.required_columns].copy()
            df_data = df_data[df_data["Количество Договоров, шт."].notna()]
            df_data["Количество Договоров, шт."] = pd.to_numeric(
                df_data["Количество Договоров, шт."], errors="coerce"
            )
            df_data = df_data[df_data["Количество Договоров, шт."] > 0]

            if df_data.empty:
                return None

            df_result = df_data.rename(
                columns={
                    "Код Инструмента": "exchange_product_id",
                    "Наименование Инструмента": "exchange_product_name",
                    "Базис поставки": "delivery_basis_name",
                    "Объем Договоров в единицах измерения": "volume",
                    "Обьем Договоров, руб.": "total",
                    "Количество Договоров, шт.": "count",
                }
            )

            df_result["exchange_product_id"] = df_result["exchange_product_id"].str.slice(0, 20)
            df_result["oil_id"] = df_result["exchange_product_id"].str[:10]
            df_result["delivery_basis_id"] = df_result["exchange_product_id"].str[4:14]
            df_result["delivery_type_id"] = df_result["exchange_product_id"].str[-5:]
            df_result["exchange_product_name"] = df_result["exchange_product_name"].str.slice(0, 1000)
            df_result["delivery_basis_name"] = df_result["delivery_basis_name"].str.slice(0, 500)

            df_result["date"] = report_date

            return df_result

        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {e}")
            return None

    def save_to_database(self, df: pd.DataFrame) -> int:
        db = SessionLocal()
        try:
            df = df.where(pd.notnull(df), None)

            records = []
            for _, row in df.iterrows():
                record = {
                    "exchange_product_id": str(row["exchange_product_id"])[:20] if row["exchange_product_id"] else None,
                    "exchange_product_name": str(row["exchange_product_name"]) if row[
                        "exchange_product_name"] else None,
                    "oil_id": str(row["oil_id"])[:10] if row["oil_id"] else None,
                    "delivery_basis_id": str(row["delivery_basis_id"])[:10] if row["delivery_basis_id"] else None,
                    "delivery_basis_name": str(row["delivery_basis_name"]) if row["delivery_basis_name"] else None,
                    "delivery_type_id": str(row["delivery_type_id"])[:5] if row["delivery_type_id"] else None,
                    "volume": float(row["volume"]) if pd.notnull(row["volume"]) else None,
                    "total": float(row["total"]) if pd.notnull(row["total"]) else None,
                    "count": int(row["count"]) if pd.notnull(row["count"]) else None,
                    "date": row["date"],
                }
                records.append(record)

            db.bulk_insert_mappings(SpimexTradingResult, records)  # оптимизация вместо insert
            db.commit()
            return len(records)
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка при сохранении в БД: {e}", exc_info=True)
            return 0
        finally:
            db.close()

    def process_directory(self, directory: Path) -> int:
        total_processed = 0
        for file_path in directory.glob("*.xls"):
            try:
                date_str = file_path.stem.split("_")[-1][:8]
                report_date = date.fromisoformat(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")

                df = self.parse_xls_file(file_path, report_date)
                if df is not None:
                    count = self.save_to_database(df)
                    total_processed += count
                    logger.info(f"Обработан файл {file_path.name}, сохранено {count} записей")
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {file_path.name}: {e}")

        return total_processed
