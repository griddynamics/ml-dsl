import argparse
import json
import time
from datetime import datetime

import gc
import pandas as pd
# noinspection PyPep8Naming
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SQLContext
from sklearn.externals import joblib
from sklearn.preprocessing import MinMaxScaler, LabelEncoder, PowerTransformer

from ai4ops_db import *
from apigee_ingest_utils import METRICS_NODE, FROM_NODE, TO_NODE, \
    METRIC_NODE, TIME_NODE, SELECT_NODE, VALUE_NODE, TASKS_NODE, ISO_TIME_FORMAT, upload_to_gs, get_bucket

SUPERVISED_LABEL = 'var1(t)'
VALUE_CLASS_NODE = 'value_class'
DAY_OF_WEEK_NODE = 'day_of_week'
HOUR_OF_DAY_NODE = 'hour_of_day'
IS_WORKING_DAY_NODE = 'is_working_day'
TRAIN_NODE = 'LGBM-TRAIN'
TEST_NODE = 'LGBM-TEST'
VAL_NODE = 'LGBM-VAL'
LABELS_NODE = 'labels'
LABEL_COLUMN_NODE = 'label_column'
PATTERN_NODE = 'pattern'
INCLUDE_NODE = 'include_pattern_groups'
LABELED_COLUMN_NODE = 'labeled_column'
METRIC_CLASS = 'metric_class'


def series_to_supervised(dt, n_in, n_out,
                         drop_nan=True,
                         index=None,
                         included_n_in=None,
                         excluded_n_in=None,
                         group_by_columns=None,
                         stationary_columns=None):
    if index is None:
        index = []
    cols, names = list(), list()
    names_map = dict()

    if stationary_columns is not None:
        group = dt.drop(stationary_columns, axis=1).groupby(group_by_columns)
    else:
        group = dt.groupby(group_by_columns)

    n_in_range = included_n_in if included_n_in is not None else range(n_in, 0, -1)
    for i in n_in_range:
        if excluded_n_in is not None and i in excluded_n_in:
            continue
        df = group.shift(i)
        df.sort_index(axis=1, inplace=True)
        df_names = df.columns.values.tolist()
        cols.append(df)
        gc.collect()
        curr_names = []
        for col in df_names:
            if col not in index:
                key = 'var%d(t-%d)' % (df_names.index(col) + 1, i)
                curr_names.append(key)
                names_map[key] = col
        names += curr_names
    for i in range(0, n_out):
        curr_names = []

        df = group.shift(i)
        df.sort_index(axis=1, inplace=True)
        df_names = df.columns.values.tolist()
        cols.append(df)
        gc.collect()

        for col in df_names:
            if col not in index:
                if i == 0:
                    key = 'var%d(t)' % (df_names.index(col) + 1)
                else:
                    key = 'var%d(t+%d)' % (df_names.index(col) + 1, i)
                curr_names.append(key)
                names_map[key] = col

        names += curr_names

    cols.append(dt[group_by_columns].sort_index(axis=1))
    names.extend(sorted(group_by_columns))

    if stationary_columns is not None:
        cols.append(dt[stationary_columns].sort_index(axis=1))
        names.extend(sorted(stationary_columns))

    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    if drop_nan:
        agg.dropna(inplace=True)
    return agg, names_map


def _get_time_index(pdf, task):
    from_time = task.get(FROM_NODE, '')
    to_time = task.get(TO_NODE, '')
    return pdf[TIME_NODE].between(from_time, to_time)


def scale(pdf, scale_index, scale_column, scl):
    if scl is None:
        scl = MinMaxScaler()
        scl.fit(pdf.loc[scale_index, scale_column].values.reshape(-1, 1))
    pdf.loc[scale_index, scale_column] = scl.transform(pdf.loc[scale_index, scale_column].values.reshape(-1, 1))
    return pdf, scl


def apply_scaling(pdf, scaler_ref, bucket_ref, suffix, bucket_name, bucket_path, allow_persist):
    first = scaler_ref is None
    not_na_index = ~pdf[VALUE_NODE].isna()
    time_index = ~pdf[TIME_NODE].isna()
    try:
        pdf, scaler_ref = scale(pdf, not_na_index & time_index, VALUE_NODE, scaler_ref)
    except RuntimeError as e:
        print(e)
        return None

    if first and allow_persist:
        scl_file = 'LGBM-SCL-{}.pkl'.format(suffix)
        joblib.dump(scaler_ref, scl_file)
        print('Writing scaler {} to GCS bucket {} path {}'.format(scl_file, bucket_name,
                                                                  bucket_path))
        upload_to_gs(bucket_ref, bucket_path, scl_file)
        print('Written scaler {} to GCS bucket {} path {}'.format(scl_file, bucket_name,
                                                                  bucket_path))
    return pdf


def label(pdf, label_index, label_column, le):
    if le is None:
        le = LabelEncoder()
        le.fit(pdf.loc[label_index, label_column].values.reshape(-1, 1))
    pdf.loc[label_index, label_column] = le.transform(pdf.loc[label_index, label_column].values.reshape(-1, 1))
    return pdf, le


def apply_power_transform(pdf, task_name, transformer):
    not_na_index = ~pdf[VALUE_NODE].isna()
    time_index = ~pdf[TIME_NODE].isna()
    idx = not_na_index & time_index
    transformer = _apply_power_transform_all_metrics(pdf, idx, VALUE_NODE, task_name, transformer)
    return pdf, transformer


def _apply_power_transform_all_metrics(pdf, transform_index, transform_column, task_name, transformer):
    values = pdf.loc[transform_index, transform_column].values.reshape(-1, 1).tolist()
    if transformer is not None:
        print("Applying the PowerTransformer for task '{}' to all metrics at once".format(task_name))
        pdf.loc[transform_index, transform_column] = transformer.transform(values)
    else:
        print("Creating new PowerTransformer for task '{}'".format(task_name))
        transformer = PowerTransformer(standardize=True)
        print("Applying the PowerTransformer for task '{}' to all metrics at once".format(task_name))
        pdf.loc[transform_index, transform_column] = transformer.fit_transform(values)
    return transformer


def upload_transformer(output_bucket, output_bucket_path, transformer, suffix):
    scl_file = 'LGBM-SCL-{}.pkl'.format(suffix)
    joblib.dump(transformer, scl_file)
    print('Writing transformer {} to GCS bucket {} path {}'.format(scl_file, output_bucket, output_bucket_path))
    upload_to_gs(output_bucket, output_bucket_path, scl_file)
    print('Written transformer {} to GCS bucket {} path {}'.format(scl_file, output_bucket, output_bucket_path))


def label_by_value_class(pdf, metrics_pdf):
    return pd.merge(pdf, metrics_pdf, on=[METRIC_NODE], how='inner')


def add_calendar_features(pdf):
    def day_of_week(dt_str):
        dt = str_to_dt(dt_str)
        return dt.weekday() + 1

    def hour_of_day(dt_str):
        dt = str_to_dt(dt_str)
        return dt.hour

    def is_working_day(dt_str):
        return day_of_week(dt_str) <= 5

    def str_to_dt(dt_str):
        return datetime.strptime(dt_str, ISO_TIME_FORMAT)

    pdf[DAY_OF_WEEK_NODE] = pdf[TIME_NODE].map(day_of_week)
    pdf[HOUR_OF_DAY_NODE] = pdf[TIME_NODE].map(hour_of_day)
    pdf[IS_WORKING_DAY_NODE] = pdf[TIME_NODE].map(is_working_day)

    return pdf, [DAY_OF_WEEK_NODE, HOUR_OF_DAY_NODE, IS_WORKING_DAY_NODE]


def make_signature(input_df,
                   config,
                   metrics_pdf,
                   metrics_labels,
                   bucket,
                   bucket_name,
                   bucket_path,
                   d_keys=None,
                   scaler=None,
                   transformer=None,
                   suffix=None,
                   with_power_transform=False,
                   with_calendar_features=False):
    train_time_ranges = config.get(TASKS_NODE, {}).get(TRAIN_NODE)
    val_time_ranges = config.get(TASKS_NODE, {}).get(VAL_NODE)
    test_time_ranges = config.get(TASKS_NODE, {}).get(TEST_NODE)
    _, transformer, scaler, d_keys = filter_and_transform(
        input_df=input_df,
        config=config,
        metrics_pdf=metrics_pdf,
        metrics_labels=metrics_labels,
        task_name=TRAIN_NODE,
        time_ranges=train_time_ranges,
        d_keys=d_keys,
        allow_persist=True,
        transformer=transformer,
        scaler=scaler,
        ignore_time_range=False,
        bucket=bucket,
        suffix=suffix,
        bucket_name=bucket_name,
        bucket_path=bucket_path,
        with_calendar_features=with_calendar_features,
        with_power_transform=with_power_transform)

    _, transformer, scaler, d_keys = filter_and_transform(
        input_df=input_df,
        config=config,
        metrics_pdf=metrics_pdf,
        metrics_labels=metrics_labels,
        task_name=VAL_NODE,
        time_ranges=val_time_ranges,
        d_keys=d_keys,
        allow_persist=True,
        transformer=transformer,
        scaler=scaler,
        ignore_time_range=False,
        bucket=bucket,
        suffix=suffix,
        bucket_name=bucket_name,
        bucket_path=bucket_path,
        with_calendar_features=with_calendar_features,
        with_power_transform=with_power_transform)

    filter_and_transform(
        input_df=input_df,
        config=config,
        metrics_pdf=metrics_pdf,
        metrics_labels=metrics_labels,
        task_name=TEST_NODE,
        time_ranges=test_time_ranges,
        d_keys=d_keys,
        allow_persist=True,
        transformer=transformer,
        scaler=scaler,
        ignore_time_range=False,
        bucket=bucket,
        suffix=suffix,
        bucket_name=bucket_name,
        bucket_path=bucket_path,
        with_calendar_features=with_calendar_features,
        with_power_transform=with_power_transform)


def filter_and_transform(input_df,
                         config,
                         metrics_pdf,
                         metrics_labels,
                         task_name,
                         time_ranges,
                         d_keys,
                         allow_persist,
                         transformer,
                         scaler,
                         ignore_time_range,
                         bucket,
                         suffix,
                         bucket_name,
                         bucket_path,
                         with_calendar_features,
                         with_power_transform):
    frames = []
    for time_range in time_ranges:
        from_time = time_range.get(FROM_NODE, None)
        to_time = time_range.get(TO_NODE, None)
        if from_time is None or to_time is None:
            raise ValueError('Invalid time range')
        # noinspection PyUnresolvedReferences
        pdf = input_df.filter(F.col(TIME_NODE).between(from_time, to_time)).toPandas()
        pdf = pdf[[TIME_NODE, VALUE_NODE, METRIC_NODE]].sort_values([METRIC_NODE, TIME_NODE])
        pdf.columns = [TIME_NODE, VALUE_NODE, METRIC_NODE]
        if len(pdf) > 0:
            frames.append(pdf)
    if len(frames) > 0:
        pdf = pd.concat(frames, axis=0, ignore_index=True)
        gc.collect()
    else:
        return None, transformer, scaler, d_keys
    return transform(pdf,
                     config,
                     metrics_pdf,
                     metrics_labels,
                     task_name,
                     time_ranges,
                     d_keys,
                     allow_persist,
                     transformer,
                     scaler,
                     ignore_time_range,
                     bucket,
                     suffix,
                     bucket_name,
                     bucket_path,
                     with_calendar_features,
                     with_power_transform)


def transform(pdf,
              config,
              metrics_pdf,
              metrics_labels,
              task_name,
              time_ranges,
              d_keys,
              allow_persist,
              transformer,
              scaler,
              ignore_time_range,
              bucket,
              suffix,
              output_bucket,
              output_bucket_path,
              with_calendar_features,
              with_power_transform):
    calendar_columns = None
    if with_calendar_features:
        pdf, calendar_columns = add_calendar_features(pdf)
    lags = config.get(SELECT_NODE, None)
    pdf = label_by_value_class(pdf, metrics_pdf)
    gc.collect()

    if with_power_transform:
        first = transformer is None
        pdf, transformer = apply_power_transform(pdf, task_name, transformer)
        if allow_persist and transformer is not None and first:
            upload_transformer(bucket, output_bucket_path, transformer, suffix)
    else:
        pdf = apply_scaling(pdf, scaler, bucket, suffix,
                            output_bucket, output_bucket_path, allow_persist)
        if pdf is None:
            return None, transformer, scaler, d_keys
    stationary_columns = [TIME_NODE]
    if metrics_labels is not None and len(metrics_labels) > 0:
        stationary_columns.extend(metrics_labels)

    if calendar_columns and len(calendar_columns) > 0:
        stationary_columns.extend(calendar_columns)

    if not ignore_time_range:
        frames = []
        names_map = dict()
        for time_range in time_ranges:
            time_index = _get_time_index(pdf, time_range)
            i_pdf = pdf.loc[time_index, :]
            s_pdf, i_names_map = series_to_supervised(i_pdf,
                                                      n_in=5,
                                                      n_out=1,
                                                      drop_nan=False,
                                                      included_n_in=lags,
                                                      group_by_columns=[METRIC_NODE],
                                                      stationary_columns=stationary_columns)
            if s_pdf is None or len(s_pdf) == 0:
                continue
            frames.append(s_pdf)
            names_map.update(i_names_map)
        if len(frames) == 0:
            return None, transformer, scaler, d_keys
        s_pdf = pd.concat(frames, axis=0, ignore_index=True)
    else:
        s_pdf, names_map = series_to_supervised(pdf,
                                                n_in=5,
                                                n_out=1,
                                                drop_nan=False,
                                                included_n_in=lags,
                                                group_by_columns=[METRIC_NODE],
                                                stationary_columns=stationary_columns)
    del pdf
    gc.collect()
    s_pdf = s_pdf[np.isfinite(s_pdf[SUPERVISED_LABEL])].reset_index(drop=True)
    if len(s_pdf) == 0:
        return None, transformer, scaler, d_keys
    if d_keys is None:
        d = (s_pdf.ne(s_pdf.iloc[0])).any()
        d_keys = d[~d].keys().values
        first = True
    else:
        first = False

    if len(d_keys) > 0:
        s_pdf.drop(d_keys, axis=1, inplace=True)

    gc.collect()

    if allow_persist:
        if first:
            d_keys_file = 'LGBM-DROP-KEYS-{}.txt'.format(suffix)
            with open(d_keys_file, 'w') as f:
                f.write(','.join(d_keys) if d_keys is not None and len(d_keys) > 0 else '')
            print('Writing {} to GCS bucket {} path {}'.format(d_keys_file, output_bucket, output_bucket_path))
            upload_to_gs(bucket, output_bucket_path, d_keys_file)
            print('Written {} to GCS bucket {} path {}'.format(d_keys_file, output_bucket, output_bucket_path))
        split_file = '{}-{}.csv'.format(task_name, suffix)
        print("Writing {} to a local csv file".format(split_file))
        s_pdf.to_csv(split_file, index=False)
        print('Uploading {} to GCS bucket {} path {}'.format(split_file, output_bucket, output_bucket_path))
        upload_to_gs(bucket, output_bucket_path, split_file)
        print('Uploaded {} to GCS bucket {} path {}'.format(split_file, output_bucket, output_bucket_path))
        gc.collect()
    return s_pdf, transformer, scaler, d_keys


def create_metric_labels(metrics_pdf, labels):
    labeled = []
    if labels is not None and len(labels) > 0:
        for lbl in labels:
            i_pattern = lbl.get(PATTERN_NODE, None)
            i_include = lbl.get(INCLUDE_NODE, None)
            i_labeled_column = lbl.get(LABELED_COLUMN_NODE, None)
            if i_pattern is None or i_labeled_column is None or i_include is None:
                print(
                    'Invalid label configuration pattern = {},  labeled_column = {}, include = {}'.format(
                        i_pattern,
                        i_labeled_column,
                        i_include
                    ))
                continue
            i_include = [int(i) for i in i_include]
            classes = metrics_pdf[METRIC_NODE].str.extract(i_pattern, expand=True)

            # TODO: nans are float not strings
            def join_pattern_groups(row):
                return '-'.join(row) if ~row.isna().any() else np.nan

            classes = classes[i_include].apply(lambda row: join_pattern_groups(row) , axis=1)
            # val_column = '{}_val'.format(i_labeled_column)
            metrics_pdf[i_labeled_column] = classes
            metrics_pdf[i_labeled_column].fillna(metrics_pdf[METRIC_NODE], inplace=True)
            metrics_pdf[i_labeled_column] = metrics_pdf.groupby([i_labeled_column]).grouper.label_info
            labeled.append(i_labeled_column)
    if len(labeled) == 0:
        metrics_pdf[METRIC_CLASS] = metrics_pdf.reset_index().index
        labeled.append(METRIC_CLASS)
    return metrics_pdf, labeled


if __name__ == "__main__":
    sc = SparkContext(appName="partial_signature_lgbm").getOrCreate()
    sc.addPyFile('yarn_logging.py')
    import yarn_logging
    logger = yarn_logging.YarnLogger()
    sql_context = SQLContext(sc)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_data_path', type=str, help='comma separated input data paths')
    parser.add_argument('--config', type=str, help='comma separated input data paths')
    parser.add_argument('--output_bucket_project', type=str, help='output bucket project')
    parser.add_argument('--output_bucket', type=str, help='output bucket')
    parser.add_argument('--output_bucket_path', type=str, help='output bucket path')
    parser.add_argument('--with_calendar_features', type=str, help='is adding calendar features needed',
                        default='false')
    parser.add_argument('--workflow_id', type=str, help='workflow ID', default=str(int(time.time() * 1000.0)))
    parser.add_argument('--with_power_transform', type=str,
                        help='apply power transformation of metrics values instead of MinMax scaler',
                        default='false')
    parser.add_argument('--scaler_name', type=str, help='Use already existing scaler (PKL file name)', default=None)
    # ai4ops - main - storage - bucket / nd_models / lgbm
    args = parser.parse_args()

    wcf = args.with_calendar_features.strip().lower() == 'true'
    wpt = args.with_power_transform.strip().lower() == 'true'

    scaler_name = args.scaler_name
    scl = None
    trf = None
    if scaler_name is not None and scaler_name:
        if wpt:
            trf = joblib.load(scaler_name)
        else:
            scl = joblib.load(scaler_name)

    with open(args.config, 'r') as f:
        cfg = json.load(f)

    # sc = spark.sparkContext
    df = (sql_context.read.format("csv").
          option("header", "false").
          schema(DB.metrics_schema()).
          option('delimiter', ',').
          load(args.input_data_path.split(',')))

    bct = get_bucket(args.output_bucket, args.output_bucket_project)
    sfx = args.workflow_id

    metrics = cfg.get(METRICS_NODE, [])
    # noinspection PyUnresolvedReferences
    df = df.filter(F.col(METRIC_NODE).isin(metrics))

    labels = cfg.get(LABELS_NODE, [{
        LABEL_COLUMN_NODE: METRIC_NODE,
        PATTERN_NODE: '(.*)',
        INCLUDE_NODE: [0],
        LABELED_COLUMN_NODE: METRIC_CLASS
    }])

    metrics = pd.DataFrame(metrics, columns=[METRIC_NODE]).sort_values([METRIC_NODE])
    metrics, metrics_labels = create_metric_labels(metrics, labels)

    make_signature(df,
                   cfg,
                   metrics,
                   metrics_labels,
                   bct,
                   args.output_bucket,
                   args.output_bucket_path,
                   d_keys=None,
                   scaler=scl,
                   transformer=trf,
                   suffix=sfx,
                   with_power_transform=wpt,
                   with_calendar_features=wcf)

    print('Completed job.')
