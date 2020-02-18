#!/usr/bin/python
import numpy as np
import copy
import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from cryptography.fernet import Fernet
import json
from google.cloud import storage

# Constants
ENV_NODE = 'environments'
JS_RESPONSE_NODE = 'Response'
JS_TIMEUNIT_NODE = 'TimeUnit'
JS_STATS_NODE = 'stats'
JS_DATA_NODE = 'data'
JS_IDENTIFIER_NODE = 'identifier'
JS_ENV_NODE = 'env'
JS_NAMES_NODE = 'names'

DIM_NODE = 'dimensions'
METRICS_NODE = 'metrics'
PREDICTIONS_NODE = 'predictions'
RESIDUALS_NODE = 'residuals'
THRESHOLDS_NODE = 'thresholds'
ALERTS_NODE = 'alerts'
CURRENT_NODE = 'current'
METRIC_NODE = 'metric'
METRIC_VAL_NODE = '{}_val'.format(METRIC_NODE)
NAME_NODE = 'name'
MU_NODE = 'mu'
SIGMA_NODE = 'sigma'
VALUES_NODE = 'values'
VALUE_NODE = 'value'
TOTAL_LATENCY_NODE = 'totalLatency'
TARGET_LATENCY_NODE = 'targetLatency'
TARGET_STATUS_CODE_NODE = 'targetStatusCode'
STATUS_CODE_NODE = 'statusCode'
TARGET_NODE = 'target'
COUNT_NODE = 'count'
RATE_NODE = 'rate'
TPS_NODE = 'tps'

PROXY_HOST = 'proxy_host'
PROXY_PORT = 'proxy_port'
PROXY_USERNAME = 'proxy_username'
PROXY_PASSWORD = 'proxy_password'

TIME_RANGE_NODE = 'timeRange'
PAGE_SIZE = 'page_size'
TASK_NODE = 'task'
TASKS_NODE = 'tasks'
FROM_NODE = 'from'
TO_NODE = 'to'
PROXIES_NODE = 'proxies'
PROXY_NODE = 'proxy'
PERCENTILE_NODE = 'percentile'
SELECT_NODE = 'select'
TIME_NODE = 'time'
TIMESTAMP_NODE = 'timestamp'
SOURCE_NODE = 'source'
ORG_NODE = 'org'
ENV0_NODE = 'env'
DROP_OUTLIERS_PERCENTAGE = 'drop_outliers_percentage'

RESULTS_NODE = 'results'
SERIES_NODE = 'series'
COLUMNS_NODE = 'columns'
TAGS_NODE = 'tags'

MINUTE_UNIT = 'minute'

LATENCY_METRICS_TEMPLATE = 'LATENCY_METRICS'
TRAFFIC_METRICS_TEMPLATE = 'TRAFFIC_METRICS__'
TRAFFIC_METRICS_BY_IP_TEMPLATE = 'MESSAGE_COUNT_BY_IP_STAT'

TIME_FORMAT = '%m/%d/%Y %H:%M'
ISO_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
NATIVE_TIME_FORMAT = '%m/%d/%Y %H:%M'
UTC = 'UTC'

APIGEE_STATS_URL = 'https://api.enterprise.apigee.com/v1/organizations/{}/environments/{}/stats/{}?'
APIGEE_METRICS_URL = 'https://apimonitoring.enterprise.apigee.com/metrics/{}?'
APIGEE_EXTENSIONS_URL = 'https://api.enterprise.apigee.com/v1/organizations/{}/environments/{}/extensions'
APIGEE_EXTENSION_PACKS_URL = 'https://api.enterprise.apigee.com/extensionpackages'
APIGEE_APIS_URL = 'https://api.enterprise.apigee.com/v1/organizations/{}/apis'
TOKEN_FILE = 'token.json'
APIGEE_ACCESS_TOKEN = 'apigee_access_token'
ACCESS_TOKEN = 'access_token'
APIGEE_REFRESH_TOKEN = 'apigee_refresh_token'
REFRESH_TOKEN = 'refresh_token'
APIGEE_USERNAME = 'apigee_username'
APIGEE_PASSWORD = 'apigee_password'
APIGEE_OAUTH_URL = 'apigee_oauth_url'
APIGEE_ORG_NAME = 'apigee_org_name'
APIGEE_ENV_NAME = 'apigee_env_name'
APIGEE_MFA_TOKEN = 'apigee_mfa_token'
APIGEE_LATENCY_METRIC = 'latency'
APIGEE_TRAFFIC_METRIC = 'traffic'
GCP_PROJECT_ID = 'gcp_project_id'
APIGEE_IP_DIMENSIONS = 'apiproxy,ax_resolved_client_ip'


def get_bucket(bucket_id, project):
    client = storage.Client(project)
    return client.get_bucket(bucket_id)


def upload_to_gs(bct, folder_path_gs, file):
    blob = bct.blob(folder_path_gs + '/' + file)
    blob.upload_from_filename(file)


def read_credentials(sql_context, cred_file_gcs_path, prefix, res_path):
    cred_rows = sql_context.read.text(cred_file_gcs_path).collect()
    cred_file_path = '{}.log'.format(prefix)
    with open(cred_file_path, 'w') as f:
        f.write(cred_rows[0][0])
    return json.loads(ApigeeIngest.dcr(res_path + '/resource.txt', cred_file_path).decode('utf-8'))


# Class which encapsulates properties and methods to fetch data from Apigee
class ApigeeIngest:
    session = None

    @staticmethod
    def get_session():
        if ApigeeIngest.session is None:
            ApigeeIngest.session = requests.Session()
        return ApigeeIngest.session

    @staticmethod
    def renew_session():
        ApigeeIngest.session = requests.Session()
        return ApigeeIngest.session

    @staticmethod
    def get_new_access_token(token):
        username = token.get(APIGEE_USERNAME, None)
        password = token.get(APIGEE_PASSWORD, None)
        mfa_token = token.get(APIGEE_MFA_TOKEN, None)
        oauth_url = token.get(APIGEE_OAUTH_URL, None)
        if mfa_token is None:
            params = {}
        else:
            params = {
                'mfa_token': mfa_token
            }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
            'Accept': 'application/json;charset=utf-8',
            'Authorization': 'Basic ZWRnZWNsaTplZGdlY2xpc2VjcmV0',
            'Cache-Control': 'no-cache'
        }
        data = 'username={}&password={}&grant_type=password'.format(username, password)
        r = ApigeeIngest.get_session().post(url=oauth_url,
                                            params=params,
                                            headers=headers,
                                            data=data,
                                            proxies=ApigeeIngest.through_proxy(token),
                                            verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def refresh_access_token(token):
        oauth_url = token.get(APIGEE_OAUTH_URL, None)
        params = {}
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
            'Accept': 'application/json;charset=utf-8',
            'Authorization': 'Basic ZWRnZWNsaTplZGdlY2xpc2VjcmV0'
        }

        data = 'grant_type=refresh_token&refresh_token={}'.format(token.get(APIGEE_REFRESH_TOKEN, None))
        r = ApigeeIngest.get_session().post(url=oauth_url,
                                            params=params,
                                            headers=headers,
                                            data=data,
                                            proxies=ApigeeIngest.through_proxy(token),
                                            verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def get_with_token(token_wrapper, method, **kwargs):
        r_status_code, r_json = ApigeeIngest.retry(3, 5, method, **kwargs)
        is_new_token = False
        if r_status_code == 401:
            r_status_code, new_token = ApigeeIngest.refresh_access_token(token_wrapper)
            if r_status_code == 200:
                access_token = new_token.get(ACCESS_TOKEN, None)
                refresh_token = new_token.get(REFRESH_TOKEN, None)
                if refresh_token is not None and access_token is not None:
                    token_wrapper[APIGEE_ACCESS_TOKEN] = access_token
                    token_wrapper[APIGEE_REFRESH_TOKEN] = refresh_token
                    is_new_token = True
                else:
                    r_status_code = 401

            if r_status_code == 401:
                r_status_code, new_token = ApigeeIngest.get_new_access_token(token_wrapper)
                if r_status_code == 200:
                    access_token = new_token.get(ACCESS_TOKEN, None)
                    refresh_token = new_token.get(REFRESH_TOKEN, None)
                    if refresh_token is not None and access_token is not None:
                        token_wrapper[APIGEE_ACCESS_TOKEN] = access_token
                        token_wrapper[APIGEE_REFRESH_TOKEN] = refresh_token
                        is_new_token = True
            r_status_code, r_json = ApigeeIngest.retry(3, 5, method, **kwargs)
        return r_status_code, r_json, token_wrapper, is_new_token

    @staticmethod
    def retry(max_tries, delay_secs, method, **kwargs):
        tries = 0
        while tries < max_tries:
            try:
                r_status_code, r_json = method(**kwargs)
                if r_status_code == 504:
                    ApigeeIngest.renew_session()
                    print('Try {} is failed. 504 Gateway Timeout Error. Retry operation after {} secs.'.format(tries,
                                                                                                               2 * delay_secs))
                    tries += 1
                    sleep(2 * delay_secs)
                    continue
                return r_status_code, r_json
            except requests.exceptions.RequestException as e:
                ApigeeIngest.renew_session()
                print(str(e))
                print('\nTry {} is failed. Retry operation after {} secs.'.format(tries, delay_secs))
                tries += 1
                sleep(delay_secs)
        if tries == max_tries:
            print('\nOperation is failed and number of tries {} has been exceeded.'.format(max_tries))
        return 500, {}

    @staticmethod
    def through_proxy(token, protocol='https'):
        proxy_host = token.get(PROXY_HOST, None)
        proxy_port = token.get(PROXY_PORT, None)
        proxy_username = token.get(PROXY_USERNAME, None)
        proxy_password = token.get(PROXY_PASSWORD, None)

        if proxy_host is None or proxy_port is None:
            print('No proxy')
            return {}
        if proxy_username is None or proxy_password is None:
            print('Proxy with no credentials')
            return {
                protocol: 'http://{}:{}'.format(proxy_host, proxy_port)
            }
        print('Proxy with credentials')
        return {
            protocol: 'http://{}:{}@{}:{}'.format(proxy_username, proxy_password, proxy_host, proxy_port)
        }

    @staticmethod
    def apigee_get_proxies(token):
        org_name = token.get(APIGEE_ORG_NAME, None)
        url = APIGEE_APIS_URL.format(org_name)
        headers = {
            'Authorization': 'Bearer {}'.format(token.get(APIGEE_ACCESS_TOKEN, None)),
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        }

        r = ApigeeIngest.get_session().get(url=url,
                                           headers=headers,
                                           proxies=ApigeeIngest.through_proxy(token),
                                           verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def apigee_get_stats(token,
                         api_proxies,
                         dimension_names,
                         select,
                         date_range,
                         limit,
                         offset,
                         sort_by,
                         logger):
        org_name = token.get(APIGEE_ORG_NAME, None)
        env_name = token.get(APIGEE_ENV_NAME, None)

        url = APIGEE_STATS_URL.format(org_name, env_name, dimension_names)
        params = {
            'filter': '(apiproxy in \'{}\')'.format('\',\''.join(api_proxies)),
            'timeRange': date_range,
            'timeUnit': 'minute',
            'select': select,
            'limit': limit,
            'offset': offset,
            'sortby': sort_by,
            'tsAscending': 'true',
            'sort': 'DESC',
            '_optimized': 'js'
        }
        logger.info('Request URL: {}'.format(url))
        logger.info('Request URL params: {}'.format(params))
        headers = {
            'Authorization': 'Bearer {}'.format(token.get(APIGEE_ACCESS_TOKEN, None))
        }
        r = ApigeeIngest.get_session().get(url=url, params=params,
                                           headers=headers,
                                           proxies=ApigeeIngest.through_proxy(token),
                                           verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def apigee_latency_metrics(token,
                               api_proxies,
                               percentile,
                               from_days,
                               to_days):
        org_name = token.get(APIGEE_ORG_NAME, None)
        env_name = token.get(APIGEE_ENV_NAME, None)

        url = APIGEE_METRICS_URL.format(APIGEE_LATENCY_METRIC)
        params = {
            'org': org_name,
            'percentile': str(percentile),
            'interval': '1m',
            'windowsize': '1m',
            'select': 'totalLatency,targetLatency',
            'groupBy': 'org,env,proxy',
            'from': from_days,
            'to': to_days,
            'env': env_name,
            'proxy': ','.join(api_proxies)
        }
        headers = {
            'Authorization': 'Bearer {}'.format(token.get(APIGEE_ACCESS_TOKEN, None))
        }
        r = ApigeeIngest.get_session().get(url=url, params=params,
                                           headers=headers,
                                           proxies=ApigeeIngest.through_proxy(token),
                                           verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def apigee_traffic_metrics(token,
                               api_proxies,
                               from_days,
                               to_days,
                               status_code=None,
                               target_status_code=None):
        org_name = token.get(APIGEE_ORG_NAME, None)
        env_name = token.get(APIGEE_ENV_NAME, None)

        url = APIGEE_METRICS_URL.format(APIGEE_TRAFFIC_METRIC)
        params = {
            'org': org_name,
            'interval': '1m',
            'select': 'count,rate,tps',
            'groupBy': 'org,env,proxy',
            'from': from_days,
            'to': to_days,
            'env': env_name,
            'proxy': ','.join(api_proxies)
        }
        if status_code is not None:
            params['groupBy'] = params.get('groupBy', '') + ',statusCode'
            params['statusCode'] = status_code

        if target_status_code is not None:
            params['groupBy'] = params.get('groupBy', '') + ',target,targetStatusCode'
            params['targetStatusCode'] = target_status_code

        headers = {
            'Authorization': 'Bearer {}'.format(token.get(APIGEE_ACCESS_TOKEN, None))
        }

        r = ApigeeIngest.get_session().get(url=url, params=params,
                                           headers=headers,
                                           proxies=ApigeeIngest.through_proxy(token),
                                           verify=False)
        return r.status_code, {} if r.status_code != 200 else r.json()

    @staticmethod
    def custom_timedelta(q, unit='day'):
        if unit == 'day':
            return timedelta(days=q)
        if unit == 'hour':
            return timedelta(hours=q)
        if unit == 'minute':
            return timedelta(minutes=q)
        return timedelta(days=q)

    @staticmethod
    def granulate_task(task, by_proxy=10, by_time=60 * 24, time_format='%m/%d/%Y %H:%M', page_size=1000):
        from_date = task.get(TIME_RANGE_NODE, {}).get(FROM_NODE, None)
        to_date = task.get(TIME_RANGE_NODE, {}).get(TO_NODE, None)
        st = datetime.strptime(from_date, time_format)
        en = datetime.strptime(to_date, time_format)
        if st > en:
            print('Time range is not defined')
            return None

        proxies = task.get(PROXIES_NODE, [])
        proxy_splits = np.array_split(np.array(proxies), by_proxy)
        time_split = int((en - st) / ApigeeIngest.custom_timedelta(by_time, unit='minute'))

        tasks = []
        for proxy_split in proxy_splits:
            if len(proxy_split) == 0:
                continue
            new_task_template = {
                TASK_NODE: task.get(TASK_NODE, None),
                PROXIES_NODE: proxy_split.tolist(),
                SELECT_NODE: task.get(SELECT_NODE, None),
                TIME_RANGE_NODE: {
                    FROM_NODE: None,
                    TO_NODE: None
                },
                PAGE_SIZE: page_size
            }
            if time_split == 0:
                new_task = copy.deepcopy(new_task_template)
                new_task[TIME_RANGE_NODE][FROM_NODE] = st
                new_task[TIME_RANGE_NODE][TO_NODE] = en
                tasks.append(new_task)
            else:
                for d in range(time_split):
                    a = st + ApigeeIngest.custom_timedelta(d * by_time, unit=MINUTE_UNIT)
                    b = st + ApigeeIngest.custom_timedelta((d + 1) * by_time, unit=MINUTE_UNIT)
                    new_task = copy.deepcopy(new_task_template)
                    new_task[TIME_RANGE_NODE][FROM_NODE] = a.strftime(time_format)
                    new_task[TIME_RANGE_NODE][TO_NODE] = b.strftime(time_format)
                    tasks.append(new_task)

        return tasks

    @staticmethod
    def apigee_latency_metrics_response_to_pandas(r_json):
        dsc = []
        results = r_json.get(RESULTS_NODE, [])
        for res in results:
            series = res.get(SERIES_NODE, [])
            for ser in series:
                columns = ser.get(COLUMNS_NODE, [])
                values = ser.get(VALUES_NODE, [])
                tags = ser.get(TAGS_NODE, {})
                proxy = tags.get(PROXY_NODE, None)
                percentile = tags.get(PERCENTILE_NODE, None)
                org = tags.get(ORG_NODE, None)
                env = tags.get(ENV0_NODE, None)
                if proxy is None or percentile is None or len(columns) == 0 or len(values) == 0:
                    continue
                ds = pd.DataFrame(values, columns=columns)
                ds[SOURCE_NODE] = 'apigee-{}-{}'.format(org, env)
                ds1 = ds[[TIME_NODE, SOURCE_NODE, TOTAL_LATENCY_NODE]].copy(deep=True)
                ds1.columns = [TIME_NODE, SOURCE_NODE, VALUE_NODE]
                ds1[METRIC_NODE] = '{}-{}-{}'.format(proxy, TOTAL_LATENCY_NODE, percentile)
                ds2 = ds[[TIME_NODE, SOURCE_NODE, TARGET_LATENCY_NODE]].copy(deep=True)
                ds2.columns = [TIME_NODE, SOURCE_NODE, VALUE_NODE]
                ds2[METRIC_NODE] = '{}-{}-{}'.format(proxy, TARGET_LATENCY_NODE, percentile)
                dsc.extend([ds1, ds2])
        if len(dsc) == 0:
            return None
        dsc = pd.concat(dsc, axis=0, ignore_index=True)
        return dsc

    @staticmethod
    def apigee_traffic_by_ip_js_response_to_pandas(org_name, env_name, r_json, logger):
        response = r_json.get(JS_RESPONSE_NODE, {})
        times = response.get(JS_TIMEUNIT_NODE, [])
        if len(times) == 0:
            return None
        times = [datetime.utcfromtimestamp(float(tm) / 1000.).strftime(ISO_TIME_FORMAT) for tm in times]
        data = response.get(JS_STATS_NODE, {}).get(JS_DATA_NODE, [])
        dsc = []
        for dt in data:
            id_value = '_'.join(dt.get(JS_IDENTIFIER_NODE, {}).get(VALUES_NODE, []))
            metrics = dt.get(METRIC_NODE, [])
            for metric in metrics:
                env = metric.get(JS_ENV_NODE, '')
                if env != env_name:
                    continue
                metric_name = metric.get(NAME_NODE, '')
                values = metric.get(VALUES_NODE, [])
                if len(values) != len(times):
                    continue
                ds = pd.DataFrame(times, columns=[TIME_NODE])
                ds.loc[:, VALUE_NODE] = values
                ds.loc[:, METRIC_NODE] = '{}_{}'.format(id_value, metric_name)
                ds.loc[:, SOURCE_NODE] = 'apigee-{}-{}'.format(org_name, env_name)
                dsc.append(ds)
        if len(dsc) == 0:
            return None
        return pd.concat(dsc, axis=0, ignore_index=True)

    @staticmethod
    def apigee_traffic_by_ip_response_to_pandas(org_name, r_json):
        dsc = []
        environments = r_json.get(ENV_NODE, [])
        for env in environments:
            dimensions = env.get(DIM_NODE, [])
            env_name = env.get(NAME_NODE, '')
            for dim in dimensions:
                metrics = dim.get(METRICS_NODE, [])
                dim_name = dim.get(NAME_NODE, None)
                if dim_name is None:
                    continue
                for metric in metrics:
                    name = metric.get(NAME_NODE, '')
                    values = metric.get(VALUES_NODE, [])
                    records = []
                    for value in values:
                        tm = int(value.get(TIMESTAMP_NODE, None))
                        val = float(value.get(VALUE_NODE, None))
                        if tm is None:
                            continue
                        tm = datetime.utcfromtimestamp(float(tm) / 1000.).strftime(ISO_TIME_FORMAT)
                        records.append(
                            {
                                METRIC_NODE: '{}_{}'.format(dim_name, name),
                                TIME_NODE: tm,
                                VALUE_NODE: val,
                                SOURCE_NODE: 'apigee-{}-{}'.format(org_name, env_name)
                            }
                        )
                        ds = pd.DataFrame(records, columns=[METRIC_NODE, TIME_NODE, VALUE_NODE, SOURCE_NODE])
                        if len(ds) > 0:
                            dsc.append(ds)
        if len(dsc) == 0:
            return None
        dsc = pd.concat(dsc, axis=0, ignore_index=True)
        return dsc

    @staticmethod
    def traffic_name(proxy, target_status_code, status_code, target, metric):
        return '{}-{}-{}-{}'.format(
            proxy,
            'traffic-{}'.format(metric),
            status_code if target_status_code is None else target_status_code,
            PROXY_NODE if target is None else target
        )

    @staticmethod
    def apigee_traffic_metrics_response_to_pandas(r_json):
        dsc = []
        results = r_json.get(RESULTS_NODE, [])
        for res in results:
            series = res.get(SERIES_NODE, [])
            for ser in series:
                columns = ser.get(COLUMNS_NODE, [])
                values = ser.get(VALUES_NODE, [])
                tags = ser.get(TAGS_NODE, {})
                proxy = tags.get(PROXY_NODE, None)
                org = tags.get(ORG_NODE, None)
                env = tags.get(ENV0_NODE, None)
                target = tags.get(TARGET_NODE, None)
                target_status_code = tags.get(TARGET_STATUS_CODE_NODE, None)
                status_code = tags.get(STATUS_CODE_NODE, None)
                if proxy is None or len(columns) == 0 or len(values) == 0:
                    continue
                ds = pd.DataFrame(values, columns=columns)
                ds[SOURCE_NODE] = 'apigee-{}-{}'.format(org, env)
                ds1 = ds[[TIME_NODE, SOURCE_NODE, COUNT_NODE]].copy(deep=True)
                ds1.columns = [TIME_NODE, SOURCE_NODE, VALUE_NODE]
                ds1.loc[:, METRIC_NODE] = ApigeeIngest.traffic_name(proxy, target_status_code, status_code, target,
                                                                    COUNT_NODE)

                ds2 = ds[[TIME_NODE, SOURCE_NODE, RATE_NODE]].copy(deep=True)
                ds2.columns = [TIME_NODE, SOURCE_NODE, VALUE_NODE]
                ds2.loc[:, METRIC_NODE] = ApigeeIngest.traffic_name(proxy, target_status_code, status_code, target,
                                                                    RATE_NODE)

                ds3 = ds[[TIME_NODE, SOURCE_NODE, TPS_NODE]]
                ds3.columns = [TIME_NODE, SOURCE_NODE, VALUE_NODE]
                ds3.loc[:, METRIC_NODE] = ApigeeIngest.traffic_name(proxy, target_status_code, status_code, target,
                                                                    TPS_NODE)
                dsc.extend([ds1, ds2, ds3])
        if len(dsc) == 0:
            return None
        dsc = pd.concat(dsc, axis=0, ignore_index=True)
        return dsc

    @staticmethod
    def get_stats_with_token(token, proxies, dimension_names, select, date_range, limit, offset, sort_by, logger):
        return ApigeeIngest.get_with_token(token,
                                           ApigeeIngest.apigee_get_stats,
                                           token=token,
                                           api_proxies=proxies,
                                           dimension_names=dimension_names,
                                           select=select,
                                           date_range=date_range,
                                           limit=limit,
                                           offset=offset,
                                           sort_by=sort_by,
                                           logger=logger)

    @staticmethod
    def get_latency_metrics_with_token(token, proxies, percentile, from_date, to_date):
        return ApigeeIngest.get_with_token(token,
                                           ApigeeIngest.apigee_latency_metrics,
                                           token=token,
                                           api_proxies=proxies,
                                           percentile=percentile,
                                           from_days=from_date,
                                           to_days=to_date)

    @staticmethod
    def get_traffic_metrics_with_token(token, proxies, from_date, to_date, status_code, target_status_code):
        return ApigeeIngest.get_with_token(token,
                                           ApigeeIngest.apigee_traffic_metrics,
                                           token=token,
                                           api_proxies=proxies,
                                           from_days=from_date,
                                           to_days=to_date,
                                           status_code=status_code,
                                           target_status_code=target_status_code)

    @staticmethod
    def get_api_proxies_with_token(token):
        return ApigeeIngest.get_with_token(token,
                                           ApigeeIngest.apigee_get_proxies,
                                           token=token)

    @staticmethod
    def resume_token(token):
        return ApigeeIngest.get_with_token(token,
                                           ApigeeIngest.get_new_access_token,
                                           token=token)

    @staticmethod
    def dcr(res_file, file_path):
        with open(res_file, 'r') as res:
            resource = res.readline().strip()

        with open(file_path, 'r') as f:
            text = f.readline().strip()

        key = bytes(resource, 'utf-8')
        cipher_suite = Fernet(key)
        return cipher_suite.decrypt(bytes(text, 'utf-8'))

    @staticmethod
    def ecr(res_file, input_json_file):
        with open(res_file, 'r') as res:
            resource = res.readline().strip()

        with open(input_json_file, 'r') as f:
            input_json = json.load(f)

        key = bytes(resource, 'utf-8')
        cipher_suite = Fernet(key)
        return cipher_suite.encrypt(bytes(json.dumps(input_json), 'utf-8'))

    @staticmethod
    def delta(i, sign, unit):
        return (
            timedelta(minutes=sign * i) if unit == 'm' else (
                timedelta(seconds=sign * i) if unit == 's' else (
                    timedelta(days=sign * i) if unit == 'd' else timedelta(hours=sign * i)
                ))
        )
