from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, lit, first_value, expr, col, collect_list, struct, to_timestamp, date_sub, concat_ws
from tempo.tsdf import TSDF
from typing import Callable
from pyspark.sql import Column
from itertools import product


def lab_as_of_features(patient_event_df: DataFrame,
                       patient_lab: DataFrame,
                       lab_types: [str]):
    
    labs_tsdf = TSDF(patient_lab, ts_col="event_ts", partition_cols = ["patient_id", "lab_type"])

    patient_event_labs = patient_event_df.withColumn("lab_type", explode(lit(lab_types)))
    patient_event_labs_tsdf = TSDF(patient_event_labs, ts_col="event_ts", partition_cols = ["patient_id", "lab_type"])

    patient_as_of_labs = patient_event_labs_tsdf.asofJoin(right_tsdf=labs_tsdf,
                                                          left_prefix="patient",
                                                          right_prefix="")

    group_by_cols = ['patient_id'] + \
                    [ "patient_" + c for c in patient_event_labs.columns if c not in ["patient_id", "lab_type"]]
    
    return patient_as_of_labs.df.groupBy(group_by_cols) \
                                .pivot("lab_type") \
                                .agg(first_value("lab_value"))



def sliding_window_numeric_aggregates(patient_event_df: DataFrame,
                                      patient_lab: DataFrame,
                                      agg_funcs:[Callable[[str], Column]],
                                      lab_types: [str],
                                      windows_in_days: int):
    
    patient_lab = patient_lab.alias('pl')

    patient_event_labs = patient_event_df.alias('pe') \
                                         .withColumn("window_days", explode(lit(windows_in_days))) \
                                         .withColumnRenamed("event_ts","end_window_ts") \
                                         .withColumn("start_window_ts", date_sub(col("end_window_ts"), col("window_days")).cast("TIMESTAMP")) \
                                         .join(patient_lab.filter(col("pl.lab_type").isin(lab_types)),
                                               (patient_lab.patient_id == patient_event_df.patient_id) &
                                               (patient_lab.event_ts.between(col("start_window_ts"), col("end_window_ts"))),
                                               "leftouter") \
                                         .drop(col("pl.patient_id"), "start_window_ts") \
                                         .withColumn('lab_value', col('lab_value').cast("FLOAT"))
    
    aggs = [x("lab_value").alias(f"{x.__name__}") for x in agg_funcs]
    agg_cols = [f"{x.__name__}" for x in agg_funcs]

    grouped_labs = patient_event_labs.withColumn("days_lab", concat_ws("_", "lab_type","window_days")) \
                                     .filter(col('lab_type').isNotNull()) \
                                     .groupBy("patient_id", "days_lab", "end_window_ts") \
                                     .agg(*aggs) \
                                     .withColumn("aggregates", struct(*[col(c) for c in agg_cols])) \
                                     .drop(*agg_cols)

    pivot_labs = grouped_labs.withColumnRenamed("end_window_ts","event_ts") \
                             .groupBy("patient_id", "event_ts") \
                             .pivot("days_lab") \
                             .agg(first_value("aggregates"))

    window_cols = [c for c in pivot_labs.columns if c not in ['patient_id', 'event_ts']]
    feat_cols = [col(f'{w}.{a}').alias(f'{w}_{a}') for w, a in product(window_cols, agg_cols)]

    rslt = pivot_labs.select("patient_id", "event_ts", *feat_cols)

    return rslt



def events_based_lab_features(patient_event_df: DataFrame,
                              patient_lab: DataFrame,
                              lab_types: [str],
                              window_size_in_seconds: int):
    patient_lab = patient_lab.alias('pl')

    window_labs = patient_event_df.alias('pe') \
                                  .withColumnRenamed("event_ts","end_window_ts") \
                                  .withColumn("start_window_ts", expr(f"end_window_ts - INTERVAL {window_size_in_seconds} seconds")) \
                                  .join(patient_lab.filter(col("pl.lab_type").isin(lab_types)),
                                        (patient_lab.patient_id == patient_event_df.patient_id) &
                                        (patient_lab.event_ts.between(col("start_window_ts"), col("end_window_ts"))),
                                        "leftouter") \
                                  .drop(col("pl.patient_id"), "start_window_ts")
    grouped_labs = window_labs.groupBy("patient_id", "lab_type", "end_window_ts") \
                              .agg(collect_list(struct("event_ts", "lab_value")).alias("labs"))
    pivot_labs = grouped_labs.withColumnRenamed("end_window_ts","event_ts") \
                         .groupBy("patient_id", "event_ts") \
                         .pivot("lab_type") \
                         .agg(first_value("labs"))
    return pivot_labs