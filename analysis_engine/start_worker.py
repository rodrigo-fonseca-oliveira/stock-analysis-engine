#!/usr/bin/env python

import celery
import analysis_engine.consts as consts
import analysis_engine.work_tasks.get_celery_app as get_celery_app
import spylunking.log.setup_logging as log_utils


# Disable celery log hijacking
# https://github.com/celery/celery/issues/2509
@celery.signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    pass


log = log_utils.build_colorized_logger(
    name=consts.APP_NAME,
    log_config_path=consts.LOG_CONFIG_PATH)

log.info(f'start - {consts.APP_NAME}')

log.info(
    f'broker={consts.WORKER_BROKER_URL} backend={consts.WORKER_BACKEND_URL} '
    f'config={consts.WORKER_CELERY_CONFIG_MODULE} '
    f'include_tasks={consts.WORKER_TASKS}')

# Get the Celery app from the project's get_celery_app module
app = get_celery_app.get_celery_app(
    name=consts.APP_NAME,
    path_to_config_module=consts.WORKER_CELERY_CONFIG_MODULE,
    auth_url=consts.WORKER_BROKER_URL,
    backend_url=consts.WORKER_BACKEND_URL,
    include_tasks=consts.INCLUDE_TASKS)

log.info('starting celery')
app.start()

log.info(f'end - {consts.APP_NAME}')
