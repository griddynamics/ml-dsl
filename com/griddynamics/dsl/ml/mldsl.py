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
import json

from IPython import get_ipython
# noinspection PyUnresolvedReferences
from google.cloud import dataproc_v1
# noinspection PyUnresolvedReferences
from google.cloud.dataproc_v1.gapic.transports import job_controller_grpc_transport
from datetime import datetime
# noinspection PyUnresolvedReferences
from google.cloud.dataproc_v1.gapic import enums
from google.cloud import logging

from IPython.core.magic import magics_class, Magics, line_cell_magic, needs_local_scope, line_magic
from IPython.core.display import display, HTML, JSON, Image
from pyspark import SparkContext

from com.griddynamics.dsl.ml.settings.profiles import Profile, AIProfile
from com.griddynamics.dsl.ml.jobs.ai_job import AIJobBuilder
from com.griddynamics.dsl.ml.jobs.builder import DataprocJobBuilder
from com.griddynamics.dsl.ml.models.models import ModelBuilder
from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.settings.artifacts import Artifact
from com.griddynamics.dsl.ml.py_script import PyScript
from com.griddynamics.dsl.ml.sessions import SessionFactory
from com.griddynamics.dsl.ml.settings.description import Platform
from com.griddynamics.dsl.ml.executors.executors import DataprocExecutor, AIPlatformJobExecutor

from com.griddynamics.dsl.ml.helpers import *

_py_scripts = {}
job_tracker = {}
_variables = {}
prefix = '-'
DATAPROC_JOBS_URL = 'https://console.cloud.google.com/dataproc/jobs'
STORAGE_BROWSER_URL = 'https://console.cloud.google.com/storage/browser'
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
    def py_data_dict(job):
        yarn_apps = [{'name': i.name, 'state': enums.YarnApplication.State(i.state).name,
                      'progress': i.progress, 'trackingUrl': i.tracking_url} for i in job.yarn_applications]
        return {
            'project_id': job.reference.project_id,
            'job_id': job.reference.job_id,
            'cluster_name': job.placement.cluster_name,
            'pyspark_job': {
                'main_python_file_uri': job.pyspark_job.main_python_file_uri,
                'args': list(job.pyspark_job.args)
            },
            'status': enums.JobStatus.State(job.status.state).name,
            'start_time': datetime.fromtimestamp(job.status.state_start_time.seconds),
            'driver_control_files_uri': job.driver_control_files_uri,
            'driver_output_resource_uri': job.driver_output_resource_uri,
            'yarn_applications': yarn_apps
        }

    @line_magic
    def py_data(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py', nargs='+',
                            action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='DemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--max_failures_per_hour', type=int, help='Max failures rate', default=0)
        parser.add_argument('--output_path', '-o', type=str, help='Output GCS path', default='', nargs='+',
                            action=JoinAction)
        print("Parameters string = <<<{}>>>".format(py_path))
        args = parser.parse_known_args(py_path.split())
        script_name = args[0].name
        prf_name = args[0].profile
        prf = Profile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        args_dct = ExecMagic.convert(args[1])

        builder = DataprocJobBuilder()
        session = (SessionFactory(platform=args[0].platform)
                   .build_session(job_bucket=prf.bucket, job_region=prf.region,
                                  cluster=prf.cluster,
                                  job_project_id=prf.project,
                                  ml_region=prf.ai_region,
                                  use_cloud_engine_credentials=prf.use_cloud_engine_credentials))
        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        output_path = '{}/{}'.format(args[0].output_path, job_name)
        args_dct['--output_path'] = output_path

        arguments = Arguments()
        arguments.set_args(**args_dct)
        job = (builder.files_root(prf.root_path)
               .task_script(script_name, _py_scripts)
               .job_id(job_name)
               .arguments(arguments)
               .build_job(prf))

        # noinspection PyTypeChecker
        display(HTML('<a href="{url}/{job_name}?project={project}&region={region}">{job_name}</a>'.format(
            url=DATAPROC_JOBS_URL,
            job_name=job_name,
            project=prf.project,
            region=prf.region)))
        executor = DataprocExecutor(job, session)
        res = executor.submit_job(run_async=prf.job_async)
        job_tracker[job_name] = res
        # noinspection PyTypeChecker
        display(HTML('<a href="{url}/{output_path}?project={project}">Output Data {job_name}</a>'.format(
            url=STORAGE_BROWSER_URL,
            output_path=output_path.split('gs://')[1],
            job_name=job_name,
            project=prf.project)))
        # noinspection PyTypeChecker
        display(JSON(ExecMagic.py_data_dict(res)))

        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
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
        parser.add_argument('--name', '-n', type=str, help='Train script module name', default='./', nargs='+',
                            action=JoinAction)
        parser.add_argument('--package_src', '-s', type=str, help='Package src directory', default='./', nargs='+',
                            action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='AIDemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--output_path', '-o', type=str, help='Output GCS path', default='', nargs='+',
                            action=JoinAction)
        args = parser.parse_known_args(py_path.split())
        script_name = args[0].name
        prf_name = args[0].profile
        package_src = args[0].package_src
        prf = AIProfile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        args_dct = ExecMagic.convert(args[1])

        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket, job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=prf.project,
                                                                          ml_region=prf.ai_region)
        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        output_path = '{}/{}'.format(args[0].output_path, job_name)
        args_dct['--output_path'] = output_path

        arguments = Arguments()
        arguments.set_args(**args_dct)

        training_input = {
            "region": prf.ai_region,
            "scaleTier": prf.scale_tier,
            "jobDir": output_path,
            "pythonModule": '{}.{}'.format(prf.package_name, script_name.split('.py')[0]),
            "runtimeVersion": prf.runtime_version
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
        response = executor.submit_train_job()
        job_tracker[job_name] = executor

        # noinspection PyTypeChecker
        display(HTML('<a href="{url}/{output_path}?project={project}">Output Data {job_name}</a>'.format(
            url=STORAGE_BROWSER_URL,
            output_path=output_path.split('gs://')[1],
            job_name=job_name,
            project=prf.project)))
        # noinspection PyTypeChecker
        display(JSON(response))
        metrics_png = self.download_from_gs(prf.bucket, prf.project, output_path, job_name)
        if os.path.exists(metrics_png):
            # noinspection PyTypeChecker
            display(Image(filename=metrics_png))
        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
        get_ipython().set_next_input('\n'.join(job_reference))

    @line_magic
    def py_deploy(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--model', '-n', type=str, help='Name of model', nargs='+', action=JoinAction)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--profile', '-p', type=str, help='Name of profile', default='AIDemoProfile', nargs='+',
                            action=JoinAction)
        parser.add_argument('--package_src', '-s', type=str, help='Package src directory', default='./', nargs='+',
                            action=JoinAction)
        parser.add_argument('--version_name', '-v', type=str, help='Version of model', default='v1', nargs='+',
                            action=JoinAction)
        parser.add_argument('--is_new_model', '-i', help='Is new deployment model', action='store_true', default=False)
        parser.add_argument('--artifacts', '-a', help='Artifacts of model', nargs='+', default=[])
        parser.add_argument('--custom_code', '-c', help='Additional code for your model', type=str, default=None,
                            nargs='+', action=JoinAction)
        parser.add_argument('--path_to_saved_model', '-m', help='Path to trained model', default='')
        parser.add_argument('--use_cloud_engine_credentials',
                            help='Use cloud engine credentials',
                            type=bool, default=False)

        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        use_cloud_engine_credentials = args[0].use_cloud_engine_credentials
        prf = AIProfile.get(prf_name)
        if prf is None:
            raise RuntimeError('Provide parameters profile {} does not exist.'.format(prf_name))

        path_of_model = f'gs://{prf.bucket}/{args[0].model}'

        if args[0].path_to_saved_model != '':
            GCPHelper.copy_folder_on_storage(prf.bucket, args[0].path_to_saved_model, path_of_model,
                                             use_cloud_engine_credentials=use_cloud_engine_credentials)
            print("Saved model to {}".format(path_of_model))
        args_dct = ExecMagic.convert(args[1])
        for key in args_dct.keys():
            args_dct[re.sub('--', '', key)] = args_dct.pop(key)

        args_dct['runtimeVersion'] = prf.runtime_version
        args_dct['deploymentUri'] = f"{path_of_model}"

        deployment_artifacts = []
        for a in args[0].artifacts:
            if a.startswith("gs://"):
                deployment_artifact = Artifact(file_name=a, path=path_of_model)
            else:
                fname = os.path.basename(a)
                deployment_artifact = Artifact(file_name=fname, path=path_of_model)
            deployment_artifacts.append(deployment_artifact)
        m_builder = ModelBuilder()
        m_builder = m_builder.name(args[0].model).files_root(prf.root_path)
        if args[0].custom_code is not None:
            m_builder = m_builder.custom_predictor_path(args[0].custom_code)

        model = (m_builder.artifacts(deployment_artifacts)
                 .is_tuning(False)
                 .build())

        ai_job_builder = AIJobBuilder()
        ai_job_builder = ai_job_builder.model(model).package_dst(prf.package_dst)
        if args[0].package_src is not None:
            ai_job_builder = ai_job_builder.package_src(args[0].package_src)
        ai_job = ai_job_builder.deploy_input(args_dct).build()

        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket, job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=prf.project,
                                                                          ml_region=prf.ai_region)

        executor = AIPlatformJobExecutor(session, ai_job, wait_delay=10, wait_tries=1000)
        if args[0].is_new_model:
            response = executor.submit_deploy_model_job(args[0].version_name, create_new_model=True)
        else:
            response = executor.submit_deploy_model_job(args[0].version_name)
        job_tracker[job_name] = executor

        # noinspection PyTypeChecker
        display(HTML('<a href="{url}/{path_of_model}?project={project}">Deploy model path {job_name}</a>'.format(
            url=STORAGE_BROWSER_URL,
            path_of_model=path_of_model.split('gs://')[1],
            job_name=job_name,
            project=prf.project)))

        # noinspection PyTypeChecker
        display(JSON(response))
        job_reference = [
            '#Use job_{job_name} instance to browse job properties.'.format(job_name=job_name),
            "#job_{job_name} = job_tracker['{job_name}']".format(job_name=job_name)
        ]
        get_ipython().set_next_input('\n'.join(job_reference))

    @line_cell_magic
    def py_instance(self, line):
        _variables["instance"] = line

    @line_magic
    def py_test(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
        parser.add_argument('--model', '-n', type=str, help='Name of model version', nargs='+', action=JoinAction)
        parser.add_argument('--version', '-v', type=str, help='Version of model', default=None, nargs='+',
                            action=JoinAction)
        parser.add_argument('--profile', '-p', type=str, help='Name of profile',
                            default='AIDeployDemoProfile', nargs='+', action=JoinAction)
        parser.add_argument('--test', '-t', nargs='+', help='Test instances', default=[])
        args = parser.parse_known_args(py_path.split())
        prf_name = args[0].profile
        prf = AIProfile.get(prf_name)
        predictions = {
            "instances": json.loads(" ".join(args[0].test))
        }

        if args[0].version:
            v_name = f'projects/{prf.project}/models/{args[0].model}/versions/{args[0].version}'
        else:
            v_name = f'projects/{prf.project}/models/{args[0].model}'
        predictions["name"] = v_name

        m_builder = ModelBuilder()
        model = (m_builder
                 .name(args[0].model)
                 .is_tuning(False)
                 .build())

        job_name = '{}_{}'.format(prf.job_prefix, int(datetime.now().timestamp()))
        ai_job_builder = AIJobBuilder()
        ai_job = (ai_job_builder
                  .model(model)
                  .name(job_name)
                  .build())

        session = SessionFactory(platform=args[0].platform).build_session(job_bucket=prf.bucket,
                                                                          job_region=prf.region,
                                                                          cluster=prf.cluster,
                                                                          job_project_id=prf.project,
                                                                          ml_region=prf.ai_region)

        executor = AIPlatformJobExecutor(session, ai_job, wait_delay=10, wait_tries=1000)

        response = executor.submit_predictions_job(predictions)
        predictions = {}
        for preds in response:
            for key in preds.keys():
                if key in predictions.keys():
                    predictions[key].append(preds[key])
                else:
                    predictions[key] = [preds[key]]
        for k in predictions.keys():
            get_ipython().set_next_input('\n'.join((k, str(predictions[k]))))

    @line_magic
    def logging(self, py_path):
        parser = argparse.ArgumentParser(prefix_chars=prefix)
        parser.add_argument('--project_id', '-p', nargs='+', help='Project IDs to include', default=None)
        parser.add_argument('--filter', '-f', nargs='+', help='Project IDs to include', default=None)
        parser.add_argument('--order_by', '-r', type=str, help='One of ASC, DESC or None',
                            choices=[None, "ASC", "DESC"], default=None)
        parser.add_argument('--page_size', '-s', type=int, help='The maximum number of entries in each page of results',
                            default=None)
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


try:
    get_ipython().register_magics(ExecMagic)
except:
    print('Cannot load cell magic')
