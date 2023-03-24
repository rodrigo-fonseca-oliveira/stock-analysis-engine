"""
Running Distributed Algorithms Across Many Celery Workers

Use this module to handle algorithm backtesting (tuning)
or for live trading.

Under the hood, this is a Celery task handler
that processes jobs from a broker's
messaging queue. This allows the Analysis Engine to process
many algorithmic workloads concurrently using Celery's horizontally-
scalable worker pool architecture.

**Publish a SPY 60-day Backtest to the Distributed Analysis Engine**

::

    ./tools/run-algo-with-publishing.sh SPY 60 -w

or manually with:

::

    use_date=$(date +"%Y-%m-%d")
    ticker_dataset="${ticker}-${use_date}.json"
    history_loc="s3://algohistory/${ticker_dataset}"
    report_loc="s3://algoreport/${ticker_dataset}"
    extract_loc="s3://algoready/${ticker_dataset}"
    backtest_loc="file:/tmp/${ticker_dataset}"

    sa -t ${ticker} \
        -p ${history_loc} \
        -o ${report_loc} \
        -e ${extract_loc} \
        -b ${backtest_loc} \
        -s ${backtest_start_date} \
        -n ${use_date} \
        -w

"""

import inspect
import types
import importlib.machinery
import datetime
import celery.task as celery_task
import analysis_engine.consts as ae_consts
import analysis_engine.build_result as build_result
import analysis_engine.work_tasks.custom_task as custom_task
import analysis_engine.run_algo as run_algo
import analysis_engine.algo as ae_algo  # base algo
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


@celery_task.task(
    bind=True,
    base=custom_task.CustomTask,
    queue='run_distributed_algorithm')
def run_distributed_algorithm(
        self,
        algo_req):
    """run_distributed_algorithm

    Process an Algorithm using a Celery task that is
    processed by a Celery worker

    :param algo_req: dictionary for key/values for
        running an algorithm using Celery workers
    """

    label = algo_req.get(
        'name',
        'ae-algo')
    verbose = algo_req.get(
        'verbose_task',
        False)
    debug = algo_req.get(
        'debug',
        False)

    # please be careful logging prod passwords:
    if debug:
        log.info(f'task - {label} - start algo_req={algo_req}')
    elif verbose:
        log.info(f'task - {label} - start ')
    # end of start log

    rec = {}
    res = build_result.build_result(
        status=ae_consts.NOT_RUN,
        err=None,
        rec=rec)

    created_algo_object = None
    custom_algo_module = None
    new_algo_object = None
    use_custom_algo = False
    found_algo_module = True  # assume the BaseAlgo
    should_publish_extract_dataset = False
    should_publish_history_dataset = False
    should_publish_report_dataset = False

    ticker = algo_req.get(
        'ticker',
        'SPY')
    num_days_back = algo_req.get(
        'num_days_back',
        75)
    name = algo_req.get(
        'name',
        'ae-algo')
    algo_module_path = algo_req.get(
        'mod_path',
        None)
    module_name = algo_req.get(
        'module_name',
        'BaseAlgo')
    custom_algo_module = algo_req.get(
        'custom_algo_module',
        None)
    new_algo_object = algo_req.get(
        'new_algo_object',
        None)
    use_custom_algo = algo_req.get(
        'use_custom_algo',
        False)
    should_publish_extract_dataset = algo_req.get(
        'should_publish_extract_dataset',
        False)
    should_publish_history_dataset = algo_req.get(
        'should_publish_history_dataset',
        False)
    should_publish_report_dataset = algo_req.get(
        'should_publish_report_dataset',
        False)
    start_date = algo_req.get(
        'start_date',
        None)
    end_date = algo_req.get(
        'end_date',
        None)
    raise_on_err = algo_req.get(
        'raise_on_err',
        True)
    report_config = algo_req.get(
        'report_config',
        None)
    history_config = algo_req.get(
        'history_config',
        None)
    extract_config = algo_req.get(
        'extract_config',
        None)

    err = None
    if algo_module_path:
        found_algo_module = False
        module_name = algo_module_path.split('/')[-1]
        loader = importlib.machinery.SourceFileLoader(
            module_name,
            algo_module_path)
        custom_algo_module = types.ModuleType(
            loader.name)
        loader.exec_module(
            custom_algo_module)
        use_custom_algo = True

        for member in inspect.getmembers(custom_algo_module):
            if module_name in str(member):
                found_algo_module = True
                break
        # for all members in this custom module file
    # if loading a custom algorithm module from a file on disk

    if not found_algo_module:
        err = (
            f'{label} - unable to find custom algorithm '
            f'module={custom_algo_module} module_path={algo_module_path}')
        if algo_module_path:
            err = (
                f'{label} - analysis_engine.'
                'work_tasks.run_distributed_algorithm was unable '
                f'to find custom algorithm module={custom_algo_module} with '
                f'provided path to \n file: {algo_module_path} \n'
                '\n'
                'Please confirm '
                'that the class inherits from the BaseAlgo class like:\n'
                '\n'
                'import analysis_engine.algo\n'
                'class MyAlgo(analysis_engine.algo.BaseAlgo):\n '
                '\n'
                'If it is then please file an issue on github:\n '
                'https://github.com/AlgoTraders/stock-analysis-engine/'
                'issues/new \n\nFor now this error results in a shutdown'
                '\n')
        # if algo_module_path set

        log.error(err)
        res = build_result.build_result(
            status=ae_consts.ERR,
            err=err,
            rec=None)
        task_result = {
            'status': res['status'],
            'err': res['err'],
            'algo_req': algo_req,
            'rec': rec
        }
        return task_result
    # if not found_algo_module

    use_start_date = start_date
    use_end_date = end_date
    if not use_end_date:
        end_date = datetime.datetime.utcnow()
        use_end_date = end_date.strftime(
            ae_consts.COMMON_TICK_DATE_FORMAT)
    if not use_start_date:
        start_date = end_date - datetime.timedelta(days=num_days_back)
        use_start_date = start_date.strftime(
            ae_consts.COMMON_TICK_DATE_FORMAT)
    dataset_publish_extract = algo_req.get(
        'dataset_publish_extract',
        False)
    dataset_publish_history = algo_req.get(
        'dataset_publish_history',
        False)
    dataset_publish_report = algo_req.get(
        'dataset_publish_report',
        False)
    try:
        if use_custom_algo:
            if verbose:
                log.info(
                    f'inspecting {custom_algo_module} for class {module_name}')
            use_class_member_object = None
            for member in inspect.getmembers(custom_algo_module):
                if module_name in str(member):
                    if verbose:
                        log.info(f'start {name} with {member[1]}')
                    use_class_member_object = member
                    break
            # end of looking over the class definition but did not find it

            if use_class_member_object:
                if algo_req.get('backtest', False):
                    new_algo_object = member[1](
                        ticker=algo_req['ticker'],
                        config_dict=algo_req)
                else:
                    new_algo_object = member[1](
                        **algo_req)
            else:
                err = (
                    f'{label} - did not find a derived '
                    'analysis_engine.algo.BaseAlgo '
                    f'class in the module file={algo_module_path} '
                    f'for ticker={ticker} algo_name={name}')
                log.error(err)
                res = build_result.build_result(
                    status=ae_consts.ERR,
                    err=err,
                    rec=None)
                task_result = {
                    'status': res['status'],
                    'err': res['err'],
                    'algo_req': algo_req,
                    'rec': rec
                }
                return task_result
            # end of finding a valid algorithm object
        else:
            new_algo_object = ae_algo.BaseAlgo(
                **algo_req)
        # if using a custom module path or the BaseAlgo

        if new_algo_object:
            # heads up - logging this might have passwords in the algo_req
            # log.debug(f'{name} algorithm request: {algo_req}')
            if verbose:
                log.info(
                    f'{name} - run START ticker={ticker} '
                    f'from {use_start_date} to {use_end_date}')
            if algo_req.get('backtest', False):
                algo_res = run_algo.run_algo(
                    algo=new_algo_object,
                    config_dict=algo_req)
                created_algo_object = new_algo_object
            else:
                algo_res = run_algo.run_algo(
                    algo=new_algo_object,
                    **algo_req)
                created_algo_object = new_algo_object

            if verbose:
                log.info(
                    f'{name} - run DONE ticker={ticker} '
                    f'from {use_start_date} to {use_end_date}')
            if debug:
                if custom_algo_module:
                    log.info(
                        f'{name} - done run_algo '
                        f'custom_algo_module={custom_algo_module} '
                        f'module_name={module_name} ticker={ticker} '
                        f'from {use_start_date} to {use_end_date}')
                else:
                    log.info(
                        f'{name} - done run_algo BaseAlgo ticker={ticker} '
                        f'from {use_start_date} to {use_end_date}')
        else:
            err = (
                f'{label} - missing a derived analysis_engine.algo.BaseAlgo '
                f'class in the module file={algo_module_path} for '
                f'ticker={ticker} algo_name={name}')
            log.error(err)
            res = build_result.build_result(
                status=ae_consts.ERR,
                err=err,
                rec=None)
            task_result = {
                'status': res['status'],
                'err': res['err'],
                'algo_req': algo_req,
                'rec': rec
            }
            return task_result
        # end of finding a valid algorithm object

        if not created_algo_object:
            err = (
                f'{label} - failed creating algorithm object - '
                f'ticker={ticker} '
                f'status={ae_consts.get_status(status=algo_res["status"])} '
                f'error={algo_res["err"]} algo name={name} '
                f'custom_algo_module={custom_algo_module} '
                f'module_name={module_name} '
                f'from {use_start_date} to {use_end_date}')
            res = build_result.build_result(
                status=ae_consts.ERR,
                err=err,
                rec=None)
            task_result = {
                'status': res['status'],
                'err': res['err'],
                'algo_req': algo_req,
                'rec': rec
            }
            return task_result
        # end of stop early

        if should_publish_extract_dataset or dataset_publish_extract:
            s3_log = ''
            redis_log = ''
            file_log = ''
            use_log = 'publish'

            if (extract_config['redis_address'] and
                    extract_config['redis_db'] and
                    extract_config['redis_key']):
                redis_log = (
                    f'redis://{extract_config["redis_address"]}'
                    f'@{extract_config["redis_db"]}'
                    f'/{extract_config["redis_key"]}')
                use_log += f' {redis_log}'
            else:
                extract_config['redis_enabled'] = False
            if (extract_config['s3_address'] and
                    extract_config['s3_bucket'] and
                    extract_config['s3_key']):
                s3_log = (
                    f's3://{extract_config["s3_address"]}'
                    f'/{extract_config["s3_bucket"]}'
                    f'/{extract_config["s3_key"]}')
                use_log += f' {s3_log}'
            else:
                extract_config['s3_enabled'] = False
            if extract_config['output_file']:
                file_log = f'file:{extract_config["output_file"]}'
                use_log += f' {file_log}'

            if verbose:
                log.info(
                    f'{name} - publish - start ticker={ticker} '
                    f'algorithm-ready {use_log}')

            publish_status = created_algo_object.publish_input_dataset(
                **extract_config)
            if publish_status != ae_consts.SUCCESS:
                msg = (
                    'failed to publish algorithm-ready datasets with '
                    f'status {ae_consts.get_status(status=publish_status)} '
                    f'attempted to {use_log}')
                log.error(msg)
                res = build_result.build_result(
                    status=ae_consts.ERR,
                    err=err,
                    rec=None)
                task_result = {
                    'status': res['status'],
                    'err': res['err'],
                    'algo_req': algo_req,
                    'rec': rec
                }
                return task_result
            # end of stop early

            if verbose:
                log.info(
                    f'{name} - publish - done ticker={ticker} '
                    f'algorithm-ready {use_log}')
        # if publish the algorithm-ready dataset

        if should_publish_history_dataset or dataset_publish_history:
            s3_log = ''
            redis_log = ''
            file_log = ''
            use_log = 'publish'

            if (history_config['redis_address'] and
                    history_config['redis_db'] and
                    history_config['redis_key']):
                redis_log = (
                    f'redis://{history_config["redis_address"]}'
                    f'@{history_config["redis_db"]}'
                    f'/{history_config["redis_key"]}')
                use_log += f' {redis_log}'
            if (history_config['s3_address'] and
                    history_config['s3_bucket'] and
                    history_config['s3_key']):
                s3_log = (
                    f's3://{history_config["s3_address"]}'
                    f'/{history_config["s3_bucket"]}'
                    f'/{history_config["s3_key"]}')
                use_log += f' {s3_log}'
            if history_config['output_file']:
                file_log = f'file:{history_config["output_file"]}'
                use_log += f' {file_log}'

            if verbose:
                log.info(
                    f'{name} - publish - start ticker={ticker} trading '
                    f'history {use_log}')

            publish_status = \
                created_algo_object.publish_trade_history_dataset(
                    **history_config)
            if publish_status != ae_consts.SUCCESS:
                msg = (
                    'failed to publish trading history datasets with '
                    f'status {ae_consts.get_status(status=publish_status)} '
                    f'attempted to {use_log}')
                log.error(msg)
                res = build_result.build_result(
                    status=ae_consts.ERR,
                    err=err,
                    rec=None)
                task_result = {
                    'status': res['status'],
                    'err': res['err'],
                    'algo_req': algo_req,
                    'rec': rec
                }
                return task_result
            # end of stop early

            if verbose:
                log.info(
                    f'{name} - publish - done ticker={ticker} trading '
                    f'history {use_log}')
        # if publish an trading history dataset

        if should_publish_report_dataset or dataset_publish_report:
            s3_log = ''
            redis_log = ''
            file_log = ''
            use_log = 'publish'

            if (report_config['redis_address'] and
                    report_config['redis_db'] and
                    report_config['redis_key']):
                redis_log = (
                    f'redis://{report_config["redis_address"]}'
                    f'@{report_config["redis_db"]}'
                    f'/{report_config["redis_key"]}')
                use_log += f' {redis_log}'
            if (report_config['s3_address'] and
                    report_config['s3_bucket'] and
                    report_config['s3_key']):
                s3_log = (
                    f's3://{report_config["s3_address"]}'
                    f'/{report_config["s3_bucket"]}'
                    f'/{report_config["s3_key"]}')
                use_log += f' {s3_log}'
            if report_config['output_file']:
                file_log = f' file:{report_config["output_file"]}'
                use_log += f' {file_log}'

            if verbose:
                log.info(
                    f'{name} - publishing ticker={ticker} trading performance '
                    f'report {use_log}')

            publish_status = created_algo_object.publish_report_dataset(
                **report_config)
            if publish_status != ae_consts.SUCCESS:
                msg = (
                    'failed to publish trading performance '
                    'report datasets with '
                    f'status {ae_consts.get_status(status=publish_status)} '
                    f'attempted to {use_log}')
                log.error(msg)
                res = build_result.build_result(
                    status=ae_consts.ERR,
                    err=err,
                    rec=None)
                task_result = {
                    'status': res['status'],
                    'err': res['err'],
                    'algo_req': algo_req,
                    'rec': rec
                }
                return task_result
            # end of stop early

            if verbose:
                log.info(
                    f'{name} - publish - done ticker={ticker} trading '
                    f'performance report {use_log}')
        # if publish an trading performance report dataset

        if verbose:
            log.info(
                f'{name} - done publishing datasets for ticker={ticker} '
                f'from {use_start_date} to {use_end_date}')

        rec['history_config'] = history_config
        rec['report_config'] = report_config

        res = build_result.build_result(
            status=ae_consts.SUCCESS,
            err=None,
            rec=rec)

    except Exception as e:
        res = build_result.build_result(
            status=ae_consts.ERR,
            err=(
                'failed - run_distributed_algorithm '
                f'dict={algo_req} with ex={e}'),
            rec=rec)
        if raise_on_err:
            raise e
        else:
            log.error(f'{label} - {res["err"]}')
    # end of try/ex

    if verbose:
        log.info(
            'task - run_distributed_algorithm done - '
            f'{label} - status={ae_consts.get_status(res["status"])}')

    task_result = {
        'status': res['status'],
        'err': res['err'],
        'algo_req': algo_req,
        'rec': rec
    }
    return task_result
# end of run_distributed_algorithm
