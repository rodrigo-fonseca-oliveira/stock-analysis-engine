"""
Load an algorithm dataset from file, s3 or redis

Supported Datasets:

- ``SA_DATASET_TYPE_ALGO_READY`` - Algorithm-ready datasets

**Supported environment variables**

::

    # to show debug, trace logging please export ``SHARED_LOG_CFG``
    # to a debug logger json file. To turn on debugging for this
    # library, you can export this variable to the repo's
    # included file with the command:
    export SHARED_LOG_CFG=/opt/sa/analysis_engine/log/debug-logging.json
"""

import os
import analysis_engine.consts as ae_consts
import analysis_engine.load_algo_dataset_from_file as file_utils
import analysis_engine.load_algo_dataset_from_s3 as s3_utils
import analysis_engine.load_algo_dataset_from_redis as redis_utils
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


def load_dataset(
        algo_dataset=None,
        dataset_type=ae_consts.SA_DATASET_TYPE_ALGO_READY,
        serialize_datasets=ae_consts.DEFAULT_SERIALIZED_DATASETS,
        path_to_file=None,
        compress=False,
        encoding='utf-8',
        redis_enabled=True,
        redis_key=None,
        redis_address=None,
        redis_db=None,
        redis_password=None,
        redis_expire=None,
        redis_serializer='json',
        redis_encoding='utf-8',
        s3_enabled=True,
        s3_key=None,
        s3_address=None,
        s3_bucket=None,
        s3_access_key=None,
        s3_secret_key=None,
        s3_region_name=None,
        s3_secure=False,
        slack_enabled=False,
        slack_code_block=False,
        slack_full_width=False,
        verbose=False):
    """load_dataset

    Load an algorithm dataset from file, s3 or redis

    :param algo_dataset: optional - already loaded algorithm-ready dataset
    :param dataset_type: optional - dataset type
        (default is ``SA_DATASET_TYPE_ALGO_READY``)
    :param path_to_file: optional - path to an algorithm-ready dataset
        in a file
    :param serialize_datasets: optional - list of dataset names to
        deserialize in the dataset
    :param compress: optional - boolean flag for decompressing
        the contents of the ``path_to_file`` if necessary
        (default is ``False`` and algorithms
        use ``zlib`` for compression)
    :param encoding: optional - string for data encoding

    **(Optional) Redis connectivity arguments**

    :param redis_enabled: bool - toggle for auto-caching all
        datasets in Redis
        (default is ``True``)
    :param redis_key: string - key to save the data in redis
        (default is ``None``)
    :param redis_address: Redis connection string format: ``host:port``
        (default is ``localhost:6379``)
    :param redis_db: Redis db to use
        (default is ``0``)
    :param redis_password: optional - Redis password
        (default is ``None``)
    :param redis_expire: optional - Redis expire value
        (default is ``None``)
    :param redis_serializer: not used yet - support for future
        pickle objects in redis
    :param redis_encoding: format of the encoded key in redis

    **(Optional) Minio (S3) connectivity arguments**

    :param s3_enabled: bool - toggle for auto-archiving on Minio (S3)
        (default is ``True``)
    :param s3_key: string - key to save the data in redis
        (default is ``None``)
    :param s3_address: Minio S3 connection string format: ``host:port``
        (default is ``localhost:9000``)
    :param s3_bucket: S3 Bucket for storing the artifacts
        (default is ``dev``) which should be viewable on a browser:
        http://localhost:9000/minio/dev/
    :param s3_access_key: S3 Access key
        (default is ``trexaccesskey``)
    :param s3_secret_key: S3 Secret key
        (default is ``trex123321``)
    :param s3_region_name: S3 region name
        (default is ``us-east-1``)
    :param s3_secure: Transmit using tls encryption
        (default is ``False``)

    **(Optional) Slack arguments**

    :param slack_enabled: optional - boolean for
        publishing to slack
    :param slack_code_block: optional - boolean for
        publishing as a code black in slack
    :param slack_full_width: optional - boolean for
        publishing as a to slack using the full
        width allowed

    Additonal arguments

    :param verbose: optional - bool for increasing
        logging
    """

    use_ds = algo_dataset
    if not use_ds:
        log.info(
            f'loading {ae_consts.get_status(status=dataset_type)} from '
            f'file={path_to_file} s3={s3_key} redis={redis_key}')
    # load if not created

    supported_type = False
    if dataset_type == ae_consts.SA_DATASET_TYPE_ALGO_READY:
        supported_type = True
        if (path_to_file and
                not use_ds):
            if not os.path.exists(path_to_file):
                log.error(f'missing file: {path_to_file}')
            use_ds = file_utils.load_algo_dataset_from_file(
                path_to_file=path_to_file,
                compress=compress,
                encoding=redis_encoding,
                serialize_datasets=serialize_datasets)
        elif (s3_key and
                not use_ds):
            use_ds = s3_utils.load_algo_dataset_from_s3(
                s3_key=s3_key,
                s3_address=s3_address,
                s3_bucket=s3_bucket,
                s3_access_key=s3_access_key,
                s3_secret_key=s3_secret_key,
                s3_region_name=s3_region_name,
                s3_secure=s3_secure,
                compress=compress,
                encoding=redis_encoding,
                serialize_datasets=serialize_datasets)
        elif (redis_key and
                not use_ds):
            use_ds = redis_utils.load_algo_dataset_from_redis(
                redis_key=redis_key,
                redis_address=redis_address,
                redis_db=redis_db,
                redis_password=redis_password,
                redis_expire=redis_expire,
                redis_serializer=redis_serializer,
                compress=compress,
                encoding=redis_encoding,
                serialize_datasets=serialize_datasets)
    else:
        supported_type = False
        use_ds = None
        log.error(
            f'loading {dataset_type} from file={path_to_file} '
            f's3={s3_key} redis={redis_key}')
    # load if not created

    if not use_ds and supported_type:
        log.error(
            f'unable to load a dataset from file={path_to_file} '
            f's3={s3_key} redis={redis_key}')

    return use_ds
# end of load_dataset
