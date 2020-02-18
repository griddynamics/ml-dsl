from pyspark.sql.types import *
import re
import mysql.connector as mysql
import numpy as np


JDBC_MYSQL_PATTERN = 'jdbc:mysql://(\d+\.\d+.\d+.\d+):(\d+)'


class DB:
    TIME = 'time'
    METRIC = 'metric'
    METRIC_VAL = 'metric_val'
    PREDICTED = 'predicted'
    PREDICTED_INV = 'predicted_inv'
    VALUE = 'value'
    VALUE_INV = 'value_inv'
    RESIDUAL = 'residual'
    RESIDUAL_INV = 'residual_inv'
    SOURCE = 'source'
    METRICS_TBL = 'metrics'
    DB_FORMAT = 'jdbc'
    TBL_APPEND_FORMAT = 'append'
    ANOMALY_TIME = 'anomaly_time'
    ANOMALY_KEY = 'anomaly_key'
    VERSION = 'version'
    LEVEL = 'level'
    CORRUPTED = 'CORRUPTED'
    CRITICAL = 'CRITICAL'
    THRESHOLD_INV = 'threshold_inv'
    THRESHOLD = 'threshold'
    QUANTILE_PC = 'quantile_pc'
    TAU_LEFT = 'tau_left'
    TAU_RIGHT = 'tau_right'
    SCORE = 'score'
    SCORE_INV = 'score_inv'
    MINOR = 'MINOR'
    WINDOW_SIZE = 'window_size'
    TMP = 'tmp'
    SOURCE_NEW = 'source_new'
    VALUE_NEW = 'value_new'
    R2 = 'r2'
    R2_INV = 'r2_inv'
    MAE = 'mae'
    MAE_INV = 'mae_inv'

    def __init__(self, db_credentials, db_table=None):
        self.driver = db_credentials.get('driver', 'com.mysql.jdbc.Driver')
        self.db = db_credentials.get('db', 'ai4ops')
        self.url = db_credentials.get('url', None) + '/' + self.db
        self.dbtable = DB.METRICS_TBL if db_table is None else db_table
        self.user = db_credentials.get('user', None)
        self.password = db_credentials.get('password', None)
        m = re.match(JDBC_MYSQL_PATTERN, db_credentials.get('url', ''))
        self.host, self.port = m.groups() if m else (None, None)

    def get_spark_db_params(self):
        params = {
            'driver': self.driver,
            'url': self.url,
            'dbtable': self.dbtable,
            'user': self.user,
            'password': self.password,
            'serverTimezone': 'UTC'
        }
        return params

    def get_python_db_params(self):
        params = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.db,
            'time_zone': '+00:00',
            'pool_size': 20,
            'compress': True
        }
        return params

    @staticmethod
    def metrics_schema():
        fields = [
            StructField(DB.TIME, StringType(), False),
            StructField(DB.SOURCE, StringType(), False),
            StructField(DB.VALUE, FloatType(), True),
            StructField(DB.METRIC, StringType(), False)
        ]

        return StructType(fields)

    @staticmethod
    def eval_schema():
        fields = [
            StructField(DB.R2, FloatType(), True),
            StructField(DB.R2_INV, FloatType(), True),
            StructField(DB.MAE, FloatType(), True),
            StructField(DB.MAE_INV, FloatType(), True)
        ]

        return StructType(fields)

    @staticmethod
    def metrics_schema_names():
        return DB.metrics_schema().names

    @staticmethod
    def metrics_schema_type_patterns():
        return ['%s', '%s', '%s', '%s']

    @staticmethod
    def prediction_schema():
        fields = [
            StructField('tmp_index', IntegerType(), True),
            StructField(DB.VALUE, FloatType(), True),
            StructField(DB.METRIC, StringType(), False),
            StructField(DB.TIME, StringType(), False),
            StructField(DB.PREDICTED, FloatType(), True)
        ]

        return StructType(fields)

    @staticmethod
    def prediction_rt_schema():
        fields = [
            StructField(DB.METRIC, StringType(), False),
            StructField(DB.TIME, StringType(), False),
            StructField(DB.VALUE, FloatType(), True),
            StructField(DB.PREDICTED, FloatType(), True),
            StructField(DB.VALUE_INV, FloatType(), True),
            StructField(DB.PREDICTED_INV, FloatType(), True)
        ]

        return StructType(fields)

    @staticmethod
    def anomaly_schema():
        fields = [
            StructField(DB.METRIC, StringType(), False),
            StructField(DB.ANOMALY_KEY, StringType(), False),
            StructField(DB.ANOMALY_TIME, StringType(), False),
            StructField(DB.VERSION, StringType(), False)
        ]
        return StructType(fields)

    @staticmethod
    def anomaly_score_schema():
        fields = [
            StructField(DB.METRIC, StringType(), False),
            StructField(DB.ANOMALY_KEY, StringType(), False),
            StructField(DB.ANOMALY_TIME, StringType(), False),
            StructField(DB.LEVEL, StringType(), True),
            StructField(DB.VERSION, StringType(), False),
            StructField(DB.TAU_LEFT, FloatType(), True),
            StructField(DB.TAU_RIGHT, FloatType(), True),
            StructField(DB.THRESHOLD, FloatType(), True),
            StructField(DB.SCORE, FloatType(), False)
        ]
        return StructType(fields)

    @staticmethod
    def anomaly_threshold_schema():
        fields = [
            StructField(DB.METRIC, StringType(), False),
            StructField(DB.LEVEL, StringType(), False),
            StructField(DB.TIME, StringType(), False),
            StructField(DB.WINDOW_SIZE, IntegerType(), False),
            StructField(DB.QUANTILE_PC, FloatType(), False),
            StructField(DB.THRESHOLD, FloatType(), True),
            StructField(DB.THRESHOLD_INV, FloatType(), True)
        ]
        return StructType(fields)

    @staticmethod
    def anomaly_short_threshold_schema():
        fields = [
            StructField(DB.TIME, StringType(), False),
            StructField(DB.WINDOW_SIZE, IntegerType(), False),
            StructField(DB.THRESHOLD, FloatType(), True),
            StructField(DB.THRESHOLD_INV, FloatType(), True)
        ]
        return StructType(fields)

    @staticmethod
    def anomaly_score_schema_names():
        return DB.anomaly_score_schema().names

    def direct_upsert_to_db(self, df, df_group_by, table, table_columns_names, buffered=None):
        if df is None or len(df) == 0:
            return
        cn = None
        crs = None
        bound_query = ''
        try:
            db_params = self.get_python_db_params()
            cn = mysql.connect(**db_params)
            if buffered is None:
                crs = cn.cursor()
            else:
                crs = cn.cursor(buffered=buffered)
            query = "REPLACE INTO {} ({}) VALUES {}"

            df = df_group_by(df)
            for group_name, group in df:
                values = []
                for value in list(group.itertuples(index=False, name=None)):
                    tpl = []
                    for c in value:
                        if c is None or c != c or c == np.inf or c == -np.inf:
                            tpl.append('NULL')
                        else:
                            tpl.append('\'{}\''.format(c) if isinstance(c, str) else str(c))
                    values.append('({})'.format(','.join(tpl)))
                bound_query = query.format(table,
                                           ','.join(table_columns_names),
                                           ','.join(values))
                crs.execute(bound_query)
                cn.commit()
        except mysql.errors.DataError as e:
            print('Data Error {} on query:\n{}'.format(str(e), bound_query))
        except Exception as e:
            print('MySql error: {}'.format(e))
            raise e
        finally:
            if crs is not None:
                crs.close()
            if cn is not None:
                cn.close()

    def direct_ins_with_update_to_db(self, df, df_group_by, table,
                                     table_columns_names,
                                     table_update_columns_names,
                                     buffered=None):
        if df is None or len(df) == 0:
            return
        cn = None
        crs = None
        try:
            db_params = self.get_python_db_params()
            cn = mysql.connect(**db_params)
            if buffered is None:
                crs = cn.cursor()
            else:
                crs = cn.cursor(buffered=buffered)
            query = "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}"

            df = df_group_by(df)
            upl = ['{column} = VALUES({column})'.format(column=column) for column in table_update_columns_names]
            for group_name, group in df:
                print('Try to store {}'.format(group_name))
                values = []
                for value in list(group.itertuples(index=False, name=None)):
                    tpl = []
                    for c in value:
                        if c is None or c != c or c == np.inf or c == -np.inf:
                            tpl.append('NULL')
                        else:
                            tpl.append('\'{}\''.format(c) if isinstance(c, str) else str(c))
                    values.append('({})'.format(','.join(tpl)))
                bound_query = query.format(table,
                                           ','.join(table_columns_names),
                                           ','.join(values),
                                           ','.join(upl))

                crs.execute(bound_query)
                cn.commit()
                print('Stored {}'.format(group_name))
        except Exception as e:
            print('MySql error: {}'.format(e))
            raise e
        finally:
            if crs is not None:
                crs.close()
            if cn is not None:
                cn.close()


    def check_alert_exists(self,string,alert_table):
        cn = None
        crs = None
        try:
            db_params = self.get_python_db_params()
            cn = mysql.connect(**db_params)
            crs = cn.cursor()
            query = "SELECT EXISTS(SELECT * FROM {} WHERE alert_data='{}')"
            crs.execute(query.format(alert_table, string))
            response = crs.fetchall()
            return response[0][0]
        except Exception as e:
            print('MySql error: {}'.format(e))
            raise e
        finally:
            if crs is not None:
                crs.close()
            if cn is not None:
                cn.close()

    def insert_alert(self,db_string,alert_table,time):
        cn = None
        crs = None
        try:
            db_params = self.get_python_db_params()
            cn = mysql.connect(**db_params)
            crs = cn.cursor()
            query = "INSERT INTO {} (alert_data,time) VALUES ('{}','{}')".format(alert_table,db_string,time)
            crs.execute(query)
            cn.commit()
        except Exception as e:
            print('MySql error: {}'.format(e))
            raise e
        finally:
            if crs is not None:
                crs.close()
            if cn is not None:
                cn.close()

