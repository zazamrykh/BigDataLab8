"""
Module for preprocessing OpenFoodFacts data.
"""
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType

from src.utils.config import config
from src.utils.logger import logger


class DataPreprocessor:
    """
    Class for preprocessing OpenFoodFacts data.
    """
    def __init__(self):
        """
        Initialize data preprocessor.
        """
        self.min_values_threshold = config.getfloat('PREPROCESSING', 'min_values_threshold')
        self.remove_outliers = config.getboolean('PREPROCESSING', 'remove_outliers')
        self.outlier_threshold = config.getfloat('PREPROCESSING', 'outlier_threshold')
        self.nutriment_features = config.getlist('FEATURES', 'nutriment_features')

    def _extract_nutriments_safe(self, df: DataFrame, nutrients: list) -> DataFrame:
        """
        For each nutrient create/overwrite a flat column with its '100g' value.
        Uses try_element_at(filter(...), 1)['100g'] to avoid exceptions when array is empty.
        """
        if "nutriments" not in df.columns:
            logger.warning("'nutriments' column not present — skipping nutriment extraction.")
            return df

        for feat in nutrients:
            # generate unique variants (avoid duplicates)
            variants = list(dict.fromkeys([
                feat,
                feat.replace("_", "-"),
                feat.replace("-", " "),
                feat.replace("-", "_"),
                feat.replace("_", " ")
            ]))
            # build condition: x.name = 'v1' OR x.name = 'v2' ...
            conds = " OR ".join([f"x.name = '{v}'" for v in variants])
            # Use try_element_at to tolerate empty arrays (returns NULL when out of bounds)
            expr = f"try_element_at(filter(nutriments, x -> {conds}), 1)['100g']"
            df = df.withColumn(feat, F.expr(expr).cast(DoubleType()))
        return df


    def preprocess_data(self, df: DataFrame) -> DataFrame:
        """
        Simplified preprocessing: keep only minimal nutriment_features,
        cast/ extract them safely, fill nulls with 0, optionally remove outliers later.
        """
        logger.info("Starting simplified preprocessing...")

        # определим целевые признаки (из конфига)
        target_features = [f.strip() for f in self.nutriment_features if f.strip()]
        logger.info(f"Target nutriment features: {target_features}")

        # Если product_name — массив (как в твоём примере), привести к тексту (берём первый элемент.lang=text)
        if "product_name" in df.columns and isinstance(df.schema["product_name"].dataType, type(df.schema["product_name"].dataType)):
            # product_name в твоём файле был array(struct(lang,text)), проще взять first element's text if exists
            try:
                df = df.withColumn("product_name",
                                F.when(F.size(F.col("product_name")) > 0, F.col("product_name")[0]["text"])
                                    .otherwise(F.col("product_name"))
                                )
            except Exception:
                # если структура другая — оставляем как есть
                pass

        # 1) Если признаки уже есть как плоские столбцы, кастим их в Double
        for feat in target_features:
            if feat in df.columns:
                df = df.withColumn(feat, F.col(feat).cast(DoubleType()))

        # 2) Для отсутствующих признаков — безопасно извлечём из nutriments
        missing = [f for f in target_features if f not in df.columns]
        if missing and "nutriments" in df.columns:
            logger.info(f"Extracting missing nutriments from 'nutriments': {missing}")
            df = self._extract_nutriments_safe(df, missing)

        # 3) Теперь у нас должны быть все целевые колонки (возможно с NULL). Покажем пример
        logger.info(f"Columns after extraction: {df.columns}")
        logger.info("Sample of nutriment columns:")
        try:
            df.select([c for c in target_features if c in df.columns]).show(5, truncate=False)
        except Exception as e:
            logger.warning(f"Couldn't show sample nutriment columns: {e}")

        # 4) Логирование количества нулей по признакам (один аггрегат)
        available = [f for f in target_features if f in df.columns]
        if available:
            null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls") for c in available]
            agg_exprs = [F.count(F.lit(1)).alias("total_rows")] + null_exprs
            agg_row = df.agg(*agg_exprs).collect()[0]
            # превращаем Row в dict — безопасно и удобно
            agg_dict = agg_row.asDict()
            total_rows = int(agg_dict.get("total_rows", 0) or 0)

            for c in available:
                nnull = int(agg_dict.get(f"{c}_nulls", 0) or 0)
                pct = (nnull / total_rows * 100) if total_rows else 0.0
                logger.info(f"Feature '{c}' nulls: {nnull} ({pct:.2f}%)")

        # 5) Заполняем NULL нулями (простая и безопасная стратегия для учебного проекта)
        fill_map = {c: 0.0 for c in available}
        if fill_map:
            df = df.fillna(fill_map)
            logger.info("Filled NULLs with 0 for nutriment features.")

        # 6) (Опционально) Приводим к окончательному виду: оставляем только нужные колонки
        final_cols = ["code", "product_name"] + available
        df_final = df.select(*[c for c in final_cols if c in df.columns])

        logger.info(f"Preprocessing completed. Rows: {df_final.count()}, Columns: {df_final.columns}")
        return df_final

    def _extract_nutriment_features(self, df: DataFrame) -> DataFrame:
        """
        Extract nutriment features from the nutriments column.
        This method is kept for backward compatibility but is no longer used directly.
        The extraction is now handled in the preprocess_data method.

        Args:
            df: DataFrame with nutriments column.

        Returns:
            DataFrame with extracted nutriment features.
        """
        logger.warning("_extract_nutriment_features is deprecated, extraction is now handled in preprocess_data")

        # Create a base DataFrame with code and product name
        if "product_name_text" in df.columns:
            result_df = df.select("code", "product_name_text")
        else:
            result_df = df.select("code", "product_name")

        # Extract each nutriment feature from the nutriments array if it exists
        if "nutriments" in df.columns:
            for feature in self.nutriment_features:
                # Create an expression to find the nutriment by name and extract its 100g value
                expr = f"""
                (SELECT `100g` FROM (
                    SELECT explode(nutriments) as nutriment
                ) WHERE nutriment.name = '{feature}')
                """

                # Add the feature column to the DataFrame with _100g suffix
                result_df = result_df.withColumn(
                    f"{feature}_100g",
                    F.expr(expr).cast(DoubleType())
                )

        # Rename product_name_text to product_name for consistency if needed
        if "product_name_text" in df.columns:
            result_df = result_df.withColumnRenamed("product_name_text", "product_name")

        return result_df

    def _remove_null_values(self, df: DataFrame) -> DataFrame:
        """
        Remove rows with null values in configured feature columns.
        This implementation:
        - first checks which features actually exist in df,
        - computes total/kept rows in a single aggregation,
        - logs removed rows (absolute + percent),
        - returns filtered df (rows where all available features are not null).
        """
        # Determine which configured features actually exist in the dataframe
        logger.info(f'df columns in _remove_null_values are {df.columns}')
        df.select('nutriments').show(5, truncate=False)

        available_features = [f for f in self.nutriment_features if f in df.columns]

        if not available_features:
            logger.warning("No configured nutriment features found in DataFrame columns; skipping null removal.")
            return df

        # Build condition: all available feature columns are not null
        condition = None
        for feature in available_features:
            col_cond = F.col(feature).isNotNull()
            condition = col_cond if condition is None else (condition & col_cond)

        # Single aggregation: total rows and rows that satisfy the condition
        agg_exprs = [
            F.count(F.lit(1)).alias("total_rows"),
            F.sum(F.when(condition, 1).otherwise(0)).alias("kept_rows")
        ]
        agg_res = df.agg(*agg_exprs).collect()[0]
        total_rows = int(agg_res["total_rows"] or 0)
        kept_rows = int(agg_res["kept_rows"] or 0)

        # Compute removed rows and log safely (avoid division by zero)
        removed_rows = total_rows - kept_rows
        pct = (removed_rows / total_rows * 100) if total_rows > 0 else 0.0
        logger.info(
            f"Null removal: considered features={available_features}. "
            f"Removed {removed_rows} rows ({pct:.2f}%); kept {kept_rows}/{total_rows}."
        )

        # Return filtered dataframe
        result_df = df.filter(condition)
        return result_df


    def _remove_outliers(self, df: DataFrame) -> DataFrame:
        """
        Remove outliers from feature columns using z-score.
        Steps:
        1) compute mean/stddev for AVAILABLE features in a single select().collect()
        2) build combined z-score condition only for features with valid stddev
        3) compute counts (total/kept) in one aggregation and log
        4) return filtered df
        """
        # Determine which features are present in df
        available_features = [f for f in self.nutriment_features if f in df.columns]

        if not available_features:
            logger.warning("No configured nutriment features found in DataFrame columns; skipping outlier removal.")
            return df

        # 1) prepare aggregate expressions for mean/stddev for available features
        agg_exprs = []
        for feature in available_features:
            agg_exprs.append(F.mean(F.col(feature)).alias(f"{feature}_mean"))
            agg_exprs.append(F.stddev(F.col(feature)).alias(f"{feature}_stddev"))

        # collect stats (one job)
        stats_row = df.select(*agg_exprs).collect()[0]

        # 2) build z-score condition, skipping features with None/zero stddev
        condition = None
        used_features = []
        for feature in available_features:
            mean = stats_row.get(f"{feature}_mean")
            stddev = stats_row.get(f"{feature}_stddev")

            if stddev is None or stddev == 0:
                logger.warning(f"Feature '{feature}': stddev is None or zero ({stddev}), skipping outlier removal for this feature.")
                continue

            # build z-score expression using literal mean/stddev values
            z_cond = F.abs((F.col(feature) - F.lit(mean)) / F.lit(stddev)) < float(self.outlier_threshold)
            condition = z_cond if condition is None else (condition & z_cond)
            used_features.append(feature)

        if condition is None:
            logger.warning("No valid features (with non-zero stddev) for outlier removal. Skipping outlier removal.")
            return df

        # 3) Compute counts in a single aggregation: total and kept (non-outliers)
        agg_check = df.agg(
            F.count(F.lit(1)).alias("total_rows"),
            F.sum(F.when(condition, 1).otherwise(0)).alias("kept_rows")
        ).collect()[0]

        total_rows = int(agg_check["total_rows"] or 0)
        kept_rows = int(agg_check["kept_rows"] or 0)
        removed_rows = total_rows - kept_rows
        pct = (removed_rows / total_rows * 100) if total_rows > 0 else 0.0

        logger.info(
            f"Outlier removal: used_features={used_features}. "
            f"Removed {removed_rows} rows ({pct:.2f}%); kept {kept_rows}/{total_rows}."
        )

        # 4) filter and return
        result_df = df.filter(condition)
        return result_df
    def get_feature_columns(self) -> List[str]:
        """
        Get the list of feature columns used for clustering.

        Returns:
            List of feature column names.
        """
        return self.nutriment_features
