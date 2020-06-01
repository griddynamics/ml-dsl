# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML_PLATFORM
# Project:     ML Platform
# Description: DSL to configure and execute ML/DS pipelines

import argparse
import sys
import re
from runpy import run_path
import numpy as np
import io

from IPython import get_ipython
# noinspection PyUnresolvedReferences
from google.cloud import dataproc_v1
from google.cloud import logging

from IPython.core.magic import magics_class, Magics, line_cell_magic, needs_local_scope, line_magic
from IPython.core.display import display, HTML, JSON, Image
from pyspark import SparkContext

from com.griddynamics.dsl.ml.settings.profiles import *
from com.griddynamics.dsl.ml.jobs.ai_job import AIJobBuilder
from com.griddynamics.dsl.ml.jobs.builder import JobBuilder
from com.griddynamics.dsl.ml.models.models import ModelBuilder
from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.settings.artifacts import Artifact
from com.griddynamics.dsl.ml.sessions import SessionFactory
from com.griddynamics.dsl.ml.executors.executors import *
from com.griddynamics.dsl.ml.helpers import *

np.set_printoptions(precision=4, threshold=np.inf)
_py_scripts = {}
job_tracker = {}
prefix = '-'
DATAPROC_JOBS_URL = 'https://console.cloud.google.com/dataproc/jobs'
STORAGE_BROWSER_URL = 'https://console.cloud.google.com/storage/browser'
S3_BROWSER_URL = 'https://console.aws.amazon.com/s3/buckets'
AI_JOBS_URL = 'https://console.cloud.google.com/ai-platform/jobs'


def script_to_cell(comment, path):
    script = os.path.basename(path)
    dir_path = os.path.dirname(path)
    content = list()
    content.append('%%py_script --exec --name {script} --path {dir_path}\n'.format(
        script=script, dir_path=dir_path))
    content.append(comment)
    content.append('#!/usr/bin/python\n')
    with open(path, 'r') as f:
        s = f.readline()
        while s is not None and s != '':
            content.append(s)
            s = f.readline()
    run = ''.join(content)
    get_ipython().set_next_input(run, replace=True)


class JoinAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, " ".join(values))


@magics_class
class ExecMagic(Magics):
    @needs_local_scope
    @line_cell_magic
    def py_script(self, line, cell, local_ns=None):
        mldsl_path = Path(os.getcwd()) / '.mldsl'

        if not sys.path.__contains__(str(mldsl_path)):
            sys.path.insert(0, str(mldsl_path))

        lines = cell.split(os.linesep)

        def handle_line_breaks(line, lines):
            if str(line).strip().endswith("\\"):
                if len(lines) > 1:
                    line = line + lines[0]
                    lines = lines[1:]
                    return handle_line_breaks(line, lines)
                elif len(lines) == 1:
                    return line + lines[0], ""
                else:
                    return line, ""
            else:
                return line, os.linesep.join(lines)

        line, cell = handle_line_breaks(line, lines)

        parser = argparse.ArgumentParser()
        parser.add_argument('--exec', '-e', help='Execute task script ', action='store_true', default=False)
        parser.add_argument('--path', '-p', help='Path to save script', type=str, default=None)
        parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py')
        parser.add_argument('--output_path', '-o', type=str, help='Output path', default='')

        p_args, d = parser.parse_known_args(line.split(" "))
        d.append('--output_path')
        d.append('{}/{}'.format(p_args.output_path, datetime.now().strftime('%y-%m-%d-%H-%M-%S')))
        sys.argv = sys.argv + d
        if p_args.exec:
            try:
                exec(cell, self.shell.user_ns, local_ns)
            except Exception as e:
                print(str(e))
            finally:
                # noinspection PyProtectedMember
                if SparkContext._active_spark_context is not None:
                    # noinspection PyProtectedMember
                    SparkContext._active_spark_context.stop()
                    print('Local SparkContext has been stopped automatically')

        os.makedirs(str(mldsl_path), exist_ok=True)
        print("Temporary path: {}".format(str(mldsl_path / p_args.name)))
        with open(str(mldsl_path / p_args.name), 'w') as f:
            f.writelines(cell)

        if p_args.path:
            os.makedirs(p_args.path, exist_ok=True)
            file_path = Path(p_args.path) / p_args.name if len(p_args.path) > 0 else p_args.name
            with open(file_path, 'w') as f:
                f.writelines(cell)
        python_script = PyScript(cell, p_args.name)
        _py_scripts[p_args.name] = python_script

        return python_script

    @line_magic
    def py_script_open(self, py_path):
        parser = argparse.ArgumentParser()
        parser.add_argument('--path', '-p', help='Path to save script', type=str, default=None)
        parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py')
        parser.add_argument('--output_path', '-o', type=str, help='Output path', default='')
        p_args, d = parser.parse_known_args(py_path.split())

        mldsl_path = Path(os.getcwd()) / '.mldsl'

        if not sys.path.__contains__(str(mldsl_path)):
            sys.path.insert(0, str(mldsl_path))

        with open(os.path.join(p_args.path, p_args.name), 'r') as f:
            lines = f.readlines()
        cell = "".join(lines)

        d.append('--output_path')
        d.append('{}/{}'.format(p_args.output_path, datetime.now().strftime('%y-%m-%d-%H-%M-%S')))
        sys.argv = sys.argv + d
        os.makedirs(str(mldsl_path), exist_ok=True)
        print("Temporary path: {}".format(str(mldsl_path / p_args.name)))
        with open(str(mldsl_path / p_args.name), 'w') as f:
            f.writelines(cell)

        python_script = PyScript(cell, p_args.name)
        _py_scripts[p_args.name] = python_script

        return python_script

    @line_magic
    def py_load(self, py_path):
        script_to_cell('# %py_load {}\n'.format(py_path), py_path)

    @staticmethod
    def convert(a):
        it = iter(a)
        a_dct = {}
        cur_key = None
        for i in it:
            if i.startswith(prefix):
                a_dct[i] = []
                cur_key = i
            else:
                a_dct[cur_key].append(i)
        return {k: ' '.join(v) for k, v in a_dct.items()}

    @staticmethod
    def build_data_job(args, prf):
        script_name = args[0].name
        args_dct = ExecMagic.convert(args[1])
        builder = JobBuilder(args[0].platform)
        session = SessionFactory(platform=args[0].platform)\
            .build_session(job_bucket=prf.bucket, job_region=prf.region,
                           cluster=prf.cluster,
                           job_project_id=prf.project,
                           ml_region=None,
                           use_cloud_engine_credentials=prf.use_cloud_engine_credentials)
        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        output_path = '{}/{}'.format(args[0].output_path, job_name)
        args_dct['--output_path'] = output_path

        arguments = Arguments()
        arguments.set_args(**args_dct)
        job = (builder.files_root(prf.root_path)
               .task_script(script_name, _py_scripts)
               .job_id(job_name)
               .arguments(arguments)
               .build_job(prf, args[0].platform))

        return session, job, job_name, output_path

    @line_magic
    def py_data(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform,
                            help='Working platform')
        parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py', nargs='+',
                            action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='DemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--max_failures_per_hour', type=int, help='Max failures rate',
                            default=0)
        parser.add_argument('--output_path', '-o', type=str, help='Output GCS path', default='', nargs='+',
                            action=JoinAction)
        print("Parameters string = <<<{}>>>".format(py_path))

        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        prf = Profile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        session, job, job_name, output_path = self.build_data_job(args, prf)

        if args[0].platform == Platform.GCP:
            # noinspection PyTypeChecker
            display(HTML('<a href="{url}/{job_name}?project={project}&region={region}">{job_name}</a>'.format(
                url=DATAPROC_JOBS_URL,
                job_name=job_name,
                project=prf.project,
                region=prf.region)))
            executor = DataprocExecutor(job, session)
            res = executor.submit_job(run_async=prf.job_async)
        else:
            executor = EmrExecutor(job, session)
            res = executor.submit_job(run_async=prf.job_async)

        job_tracker[job_name] = res
        # noinspection PyTypeChecker

        display(HTML('<a href="{url}/{output_path}?{region}">Output Data {job_name}</a>'.format(
            url=STORAGE_BROWSER_URL if args[0].platform == Platform.GCP else S3_BROWSER_URL,
            output_path=output_path.split('gs://')[1] if args[0].platform == Platform.GCP
            else f"{prf.bucket}/emr/{res['placement']['cluster_id']}/steps/{res['placement']['step_id']}/",
            job_name=job_name,
            region=f'project={prf.project}' if args[0].platform == Platform.GCP else f'region={prf.region}')))

        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
        display(JSON(res))
        get_ipython().set_next_input('\n'.join(job_reference))


    @staticmethod
    def download_from_gs(bucket_name, project, output_path, job_name):
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket_name)
        output_blob = '{path}/metrics.png'.format(
            path=output_path.split('gs://{}/'.format(bucket_name))[1]
        )
        name = '/tmp/metrics_{job_name}.png'.format(job_name=job_name)
        bucket.blob(output_blob).download_to_filename(name)
        return name

    @line_magic
    def py_train(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--name', '-n', type=str, help='Train script module name',
                            default='./', nargs='+', action=JoinAction)
        parser.add_argument('--package_src', '-s', type=str, help='Package src directory',
                            default='./', nargs='+', action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile',
                            default='AIDemoProfile', nargs='+', action=JoinAction)
        parser.add_argument('--output_path', '-o', type=str, help='Output GCS path',
                            default='', nargs='+', action=JoinAction)
        args = parser.parse_known_args(py_path.split())
        script_name = args[0].name
        prf_name = args[0].profile
        package_src = args[0].package_src
        prf = Profile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        args_dct = ExecMagic.convert(args[1])
        cred = prf.use_cloud_engine_credentials
        project = prf.project if hasattr(prf, "project") else prf.job_prefix
        ai_region = prf.ai_region if hasattr(prf, "ai_region") else prf.region
        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket,
                                                                          job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=project,
                                                                          ml_region=ai_region,
                                                                          use_cloud_engine_credentials=cred)
        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        if args[0].platform == Platform.GCP:
            output_path = '{}/{}'.format(args[0].output_path, job_name)

            args_dct = {**prf.arguments, **args_dct}
            args_dct['--output_path'] = output_path

            arguments = Arguments()
            arguments.set_args(**args_dct)
            training_input = {
                "region": prf.ai_region,
                "scaleTier": prf.scale_tier,
                "jobDir": output_path,
                "pythonModule": '{}.{}'.format(prf.package_name, script_name.split('.py')[0]),
                "runtimeVersion": prf.runtime_version,
                "pythonVersion": prf.python_version
            }
            m_builder = ModelBuilder()
            model = m_builder.name(job_name).train_arguments(arguments).build()
            ai_job_builder = AIJobBuilder()
            ai_job = (ai_job_builder.model(model)
                      .package_src(package_src)
                      .package_dst('{}/{}'.format(prf.package_dst, job_name))
                      .train_input(training_input)
                      .name(job_name)
                      .job_dir(output_path)
                      .build())

            # noinspection PyTypeChecker
            display(HTML('<a href="{url}/{job_name}/charts/cpu?project={project}">{job_name}</a>'.format(
                url=AI_JOBS_URL,
                job_name=job_name,
                project=prf.project)))

            executor = AIPlatformJobExecutor(session, ai_job, 10, 1000)
        else:
            for k in args_dct.copy():
                args_dct[re.sub("--", '', k)] = args_dct[k]
                args_dct.pop(k)
            executor = SageMakerExecutor(session, prf, mode='train', 
                                         py_script_name=os.path.join(package_src, script_name),
                                         args=args_dct)

        response = executor.submit_train_job()
        
        if args[0].platform == Platform.GCP:
            job_tracker[job_name] = executor
            # noinspection PyTypeChecker
            display(HTML('<a href="{url}/{output_path}?project={project}">Output Data {job_name}</a>'.format(
                url=STORAGE_BROWSER_URL,
                output_path=output_path.split('gs://')[1],
                job_name=job_name,
                project=prf.project)))
            # noinspection PyTypeChecker
            metrics_png = self.download_from_gs(prf.bucket, prf.project, output_path, job_name)
            if os.path.exists(metrics_png):
                # noinspection PyTypeChecker
                display(Image(filename=metrics_png))
        else:
            job_tracker[job_name] = executor.executor
            display(HTML('<a href="{url}">{job_name}</a>'.format(url=response['model_data'],
                                                                             job_name=response['model_data'])))
        display(JSON(response))
        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
        get_ipython().set_next_input('\n'.join(job_reference))
            

    @line_magic
    def py_deploy(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--model', '-n', type=str, help='Name of model', nargs='+',
                            action=JoinAction)
        parser.add_argument('--platform', '-pm', type=Platform,
                            help='Working platform')
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='AIDemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--package_src', '-s', type=str, help='Package src directory', default='./', nargs='+',
                            action=JoinAction)

        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        prf = Profile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))
        cred = prf.use_cloud_engine_credentials

        if args[0].platform == Platform.GCP:
            #path_of_model = f'gs://{prf.bucket}/{args[0].model}'
            path_of_model = prf.path_to_saved_model
            #if prf.path_to_saved_model != '':
            #    if not prf.path_to_saved_model.startswith("gs://"):
            #        GCPHelper.copy_folder_on_storage(prf.bucket, prf.path_to_saved_model, path_of_model,
            #                                 use_cloud_engine_credentials=cred)
            #        print("Saved model to {}".format(path_of_model))
            args_dct = prf.arguments
            args_dct['pythonVersion'] = prf.python_version
            args_dct['runtimeVersion'] = prf.runtime_version
            args_dct['deploymentUri'] = f"{path_of_model}"

            deployment_artifacts = []
            for a in prf.artifacts:
                if a.startswith("gs://"):
                    deployment_artifact = Artifact(file_name=a, path=path_of_model)
                else:
                    fname = os.path.basename(a)
                    deployment_artifact = Artifact(file_name=fname, path=path_of_model)
                deployment_artifacts.append(deployment_artifact)

            m_builder = ModelBuilder()
            m_builder = m_builder.name(args[0].model).files_root(prf.root_path)
            if prf.custom_code is not None:
                m_builder = m_builder.custom_predictor_path(prf.custom_code)

            model = (m_builder.artifacts(deployment_artifacts)
                     .is_tuning(False)
                     .build())

            ai_job_builder = AIJobBuilder()
            ai_job_builder = ai_job_builder.model(model).package_dst(prf.package_dst)
            if args[0].package_src is not None:
                ai_job_builder = ai_job_builder.package_src(args[0].package_src)
            ai_job = ai_job_builder.deploy_input(args_dct).build()

        job_name = '{}_{}_predictor'.format(prf.job_prefix, int(datetime.now().timestamp()))
        project = prf.project if hasattr(prf, "project") else prf.job_prefix
        ai_region = prf.ai_region if hasattr(prf, "ai_region") else prf.region
        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket,
                                                                          job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=project,
                                                                          ml_region=ai_region,
                                                                          use_cloud_engine_credentials=cred)
        if args[0].platform == Platform.GCP:
            executor = AIPlatformJobExecutor(session, ai_job, wait_delay=10, wait_tries=1000)
            if prf.is_new_model:
                response = executor.submit_deploy_model_job(prf.version_name, create_new_model=True)
            else:
                response = executor.submit_deploy_model_job(prf.version_name)
            job_tracker[job_name] = executor
            # noinspection PyTypeChecker
            display(HTML('<a href="{url}/{path_of_model}?project={project}">Deploy model path {job_name}</a>'.format(
                url=STORAGE_BROWSER_URL,
                path_of_model=path_of_model.split('gs://')[1],
                job_name=job_name,
                project=prf.project)))
        else:
            script_name = args[0].model
            package_src = args[0].package_src
            #TODO: args={}
            executor = SageMakerExecutor(session, prf, mode='deploy', 
                                         py_script_name=os.path.join(package_src, script_name), args={})
            predictor, response = executor.submit_deploy_model_job()
            job_tracker[job_name] = predictor
        # noinspection PyTypeChecker
        display(JSON(response))
        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_tracker['{job_name}']".format(job_name=job_name)
        ]
        get_ipython().set_next_input('\n'.join(job_reference))


    @line_magic
    def py_test(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--profile', '-p', type=str, default='AIDeployDemoProfile',
                            nargs='+', action=JoinAction, help='Name of profile')
        parser.add_argument('--test', '-t', nargs='+', help='Test instances', default=[])
        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        prf = AIProfile.get(prf_name)

        cred = prf.use_cloud_engine_credentials

        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket,
                                                                          job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=prf.project,
                                                                          ml_region=prf.ai_region,
                                                                          use_cloud_engine_credentials=cred)

        if args[0].platform == Platform.GCP:
            predictions = {
                "instances": json.loads(" ".join(args[0].test))
            }
            if prf.version_name:
                v_name = f'projects/{prf.project}/models/{prf.model}/versions/{prf.version_name}'
            else:
                v_name = f'projects/{prf.project}/models/{prf.model}'
            predictions["name"] = v_name

            m_builder = ModelBuilder()
            model = (m_builder
                     .name(prf.model)
                     .is_tuning(False)
                     .build())
            job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
            ai_job_builder = AIJobBuilder()
            ai_job = (ai_job_builder
                      .model(model)
                      .name(job_name)
                      .build())
            executor = AIPlatformJobExecutor(session, ai_job, wait_delay=10, wait_tries=1000)
        else:
            executor = SageMakerExecutor(session, prf, mode='predict', py_script_name = None, args={})
        response = executor.submit_prediction_job(predictions)
        if args[0].platform == Platform.GCP:
            predictions = {}
            for preds in response:
                for key in preds.keys():
                    if key in predictions.keys():
                        predictions[key].append(preds[key])
                    else:
                        predictions[key] = [preds[key]]
            for k in predictions.keys():
                get_ipython().set_next_input('\n'.join((k, str(predictions[k]))))
        else:
            get_ipython().set_next_input(np.array_repr(response).replace('\n', ''))

    @line_magic
    def logging(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--project_id', '-p', nargs='+', help='Project IDs to include', default=None)
        parser.add_argument('--filter', '-f', nargs='+', help='Project IDs to include', default=None)
        parser.add_argument('--order_by', '-r', type=str, help='One of ASC, DESC or None',
                            choices=[None, "ASC", "DESC"], default=None)
        parser.add_argument('--page_size', '-s', type=int,
                            help='The maximum number of entries in each page of results', default=None)
        args = parser.parse_known_args(py_path.split())
        FILTER = " ".join(args[0].filter)
        if args[0].order_by == "ASC":
            order_by = logging.ASCENDING
        elif args[0].order_by == "DESC":
            order_by = logging.DESCENDING
        else:
            order_by = None
        logging_client = logging.Client()
        for entry in logging_client.list_entries(projects=args[0].project_id, filter_=FILTER, order_by=order_by,
                                                 page_size=args[0].page_size):
            print('\t'.join([entry.timestamp.strftime("%m/%d/%Y, %H:%M:%S"), entry.payload["message"]]))

    @line_magic
    def job_upgrade(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py', nargs='+',
                            action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='DemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--old_job_id', type=str, help='ID of old version job', default=None, nargs='+',
                            action=JoinAction)
        parser.add_argument('--validator', '-v', help='name of class Validator', type=str, nargs='+',
                            action=JoinAction)
        parser.add_argument('--validator_path', '-vp', help='path to file with class Validator', type=str, nargs='+',
                            action=JoinAction)
        parser.add_argument('--output_path', '-o', type=str, help='Output GCS path', default='', nargs='+',
                            action=JoinAction)
        print("Parameters string = <<<{}>>>".format(py_path))

        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        prf = Profile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        session, job, job_name, output_path = self.build_data_job(args, prf)

        # noinspection PyTypeChecker
        display(HTML('<a href="{url}/{job_name}?project={project}&region={region}">{job_name}</a>'.format(
            url=DATAPROC_JOBS_URL,
            job_name=job_name,
            project=prf.project,
            region=prf.region)))

        validator_module = run_path(args[0].validator_path)

        executor = JobUpgradeExecutor(job, session, args[0].old_job_id)
        res = executor.submit_upgrade_job(validator=args[0].validator,
                                          validator_path=validator_module,
                                          run_async=prf.job_async)
        job_tracker[job_name] = res
        # noinspection PyTypeChecker
        display(JSON(res))

        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
        get_ipython().set_next_input('\n'.join(job_reference))


try:
    get_ipython().register_magics(ExecMagic)
except:
    print('Cannot load cell magic')
