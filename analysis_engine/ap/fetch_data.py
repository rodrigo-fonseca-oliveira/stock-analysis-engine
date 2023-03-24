"""
Fetch data from Tradier:
https://developer.tradier.com/getting_started
"""

import json
import copy
import pandas as pd
import analysis_engine.consts as ae_consts
import analysis_engine.ap.consts as ap_consts
import analysis_engine.options_dates as opt_dates
import analysis_engine.ap.fetch_api as ap_fetch
import analysis_engine.ap.extract_df_from_redis as ap_extract
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


def fetch_data(
        work_dict,
        fetch_type=None):
    """fetch_data

    Factory method for fetching data from
    AP using an enum or string alias. Returns
    a pandas ``DataFrame`` and only supports
    one ticker at a time.

    Supported enums from: ``analysis_engine.ap.consts``

    ::

        fetch_type = FETCH_AP_CALLS
        fetch_type = FETCH_AP_PUTS

    Supported ``work_dict['ft_type']`` string values:

    ::

        work_dict['ft_type'] = 'apcalls'
        work_dict['ft_type'] = 'apputs'

    :param work_dict: dictionary of args for the Tradier api
    :param fetch_type: optional - name or enum of the fetcher to create
                       can also be a lower case string
                       in work_dict['ft_type']
    """
    use_fetch_name = None
    ticker = work_dict.get(
        'ticker',
        None)
    if not fetch_type:
        fetch_type = work_dict.get(
            'ft_type',
            None)
    if fetch_type:
        use_fetch_name = str(fetch_type).lower()

    if 'exp_date' not in work_dict:
        work_dict['exp_date'] = opt_dates.option_expiration().strftime(
            ae_consts.COMMON_DATE_FORMAT)

    log.debug(f'name={use_fetch_name} type={fetch_type} args={work_dict}')

    status_df = ae_consts.NOT_SET
    df = pd.DataFrame([{}])

    if (
            use_fetch_name == 'apcalls' or
            fetch_type == ap_consts.FETCH_AP_CALLS):
        status_df, fetch_df = ap_fetch.fetch_calls(
            work_dict=work_dict)

        if status_df == ae_consts.SUCCESS:
            log.debug(
                'call - merge df')
            work_copy = copy.deepcopy(
                work_dict)
            work_copy['ft_type'] = ap_consts.FETCH_AP_CALLS
            work_copy['fd_type'] = 'apcalls'
            if 'apcalls' in work_dict:
                work_copy['redis_key'] = work_dict['apcalls']
                work_copy['s3_key'] = f'{work_dict["apcalls"]}.json'
            else:
                work_copy['redis_key'] = f'{work_dict["redis_key"]}_apcalls'
                work_copy['s3_key'] = f'{work_dict["redis_key"]}_apcalls'
            ext_status, ext_df = \
                ap_extract.extract_option_calls_dataset(
                    work_dict=work_copy)
            if ext_status == ae_consts.SUCCESS and len(ext_df.index) > 0:
                log.debug(
                    f'call - merging fetch={len(fetch_df.index)} '
                    f'with ext={len(ext_df.index)}')
                """
                for testing compression:
                """
                """
                import sys
                print(ext_df['date'])
                print(ext_df['ask_date'])
                print(ext_df['bid_date'])
                print(ext_df['trade_date'])
                sys.exit(1)
                """
                extracted_records = json.loads(ext_df.to_json(
                    orient='records'))
                fetched_records = json.loads(fetch_df.to_json(
                    orient='records'))
                new_records = []
                dates_by_strike_dict = {}
                for ex_row in extracted_records:
                    date_strike_name = (
                        f'{ex_row["created"]}_{ex_row["strike"]}')
                    if date_strike_name not in dates_by_strike_dict:
                        new_node = {}
                        for c in ap_consts.AP_OPTION_COLUMNS:
                            if c in ex_row:
                                new_node[c] = ex_row[c]
                        # end of for all columns to copy over
                        new_node.pop('index', None)
                        new_node.pop('level_0', None)
                        new_records.append(new_node)
                        dates_by_strike_dict[date_strike_name] = True
                # build extracted records

                for ft_row in fetched_records:
                    date_strike_name = (
                        f'{ft_row["created"]}_{ft_row["strike"]}')
                    try:
                        if date_strike_name not in dates_by_strike_dict:
                            new_node = {}
                            for c in ap_consts.AP_OPTION_COLUMNS:
                                if c in ft_row:
                                    new_node[c] = ft_row[c]
                            # end of for all columns to copy over
                            new_node.pop('index', None)
                            new_node.pop('level_0', None)
                            new_records.append(new_node)
                            dates_by_strike_dict[date_strike_name] = True
                        else:
                            log.error(
                                f'already have {ticker} call - '
                                f'date={ft_row["created"]} '
                                f'strike={ft_row["strike"]}')
                    except Exception as p:
                        log.critical(f'failed fetching call with ex={p}')
                        return ae_consts.ERR, None
                    # end of adding fetched records after the extracted

                df = pd.DataFrame(new_records)
                df.sort_values(
                    by=[
                        'date',
                        'strike'
                    ],
                    ascending=True)
                log.debug(f'call - merged={len(df.index)}')
            else:
                df = fetch_df.sort_values(
                    by=[
                        'date',
                        'strike'
                    ],
                    ascending=True)
        else:
            log.warn(
                f'{ticker} - no data found for calls')
        # if able to merge fetch + last for today
    elif (
            use_fetch_name == 'apputs' or
            fetch_type == ap_consts.FETCH_AP_PUTS):
        status_df, fetch_df = ap_fetch.fetch_puts(
            work_dict=work_dict)
        if status_df == ae_consts.SUCCESS:
            log.debug(
                'put - merge df')
            work_copy = copy.deepcopy(
                work_dict)
            work_copy['ft_type'] = ap_consts.FETCH_AP_PUTS
            work_copy['fd_type'] = 'apputs'
            if 'apputs' in work_dict:
                work_copy['redis_key'] = work_dict['apputs']
                work_copy['s3_key'] = f'{work_dict["apputs"]}.json'
            else:
                work_copy['redis_key'] = f'{work_dict["redis_key"]}_apputs'
                work_copy['s3_key'] = f'{work_dict["s3_key"]}_apputs'
            ext_status, ext_df = \
                ap_extract.extract_option_puts_dataset(
                    work_dict=work_copy)
            if ext_status == ae_consts.SUCCESS and len(ext_df.index) > 0:
                log.debug(
                    f'put - merging fetch={len(fetch_df.index)} with '
                    f'ext={len(ext_df.index)}')
                """
                for testing compression:
                """
                """
                import sys
                print(ext_df['date'])
                sys.exit(1)
                """
                extracted_records = json.loads(ext_df.to_json(
                    orient='records'))
                fetched_records = json.loads(fetch_df.to_json(
                    orient='records'))
                new_records = []
                dates_by_strike_dict = {}
                for ex_row in extracted_records:
                    date_strike_name = (
                        f'{ex_row["created"]}_{ex_row["strike"]}')
                    if date_strike_name not in dates_by_strike_dict:
                        new_node = {}
                        for c in ap_consts.AP_OPTION_COLUMNS:
                            if c in ex_row:
                                new_node[c] = ex_row[c]
                        # end of for all columns to copy over
                        new_node.pop('index', None)
                        new_node.pop('level_0', None)
                        new_records.append(new_node)
                        dates_by_strike_dict[date_strike_name] = True
                # build extracted records

                for ft_row in fetched_records:
                    date_strike_name = (
                        f'{ft_row["created"]}_{ft_row["strike"]}')
                    try:
                        if date_strike_name not in dates_by_strike_dict:
                            new_node = {}
                            for c in ap_consts.AP_OPTION_COLUMNS:
                                if c in ft_row:
                                    new_node[c] = ft_row[c]
                            # end of for all columns to copy over
                            new_node.pop('index', None)
                            new_node.pop('level_0', None)
                            new_records.append(new_node)
                            dates_by_strike_dict[date_strike_name] = True
                        else:
                            log.error(
                                f'already have {ticker} put - '
                                f'date={ft_row["created"]} '
                                f'strike={ft_row["strike"]}')
                    except Exception as p:
                        log.critical(f'failed fetching puts with ex={p}')
                        return ae_consts.ERR, None
                # end of adding fetched records after the extracted

                df = pd.DataFrame(new_records)
                df.sort_values(
                    by=[
                        'date',
                        'strike'
                    ],
                    ascending=True)
                log.debug(f'put - merged={len(df.index)}')
            else:
                df = fetch_df.sort_values(
                    by=[
                        'date',
                        'strike'
                    ],
                    ascending=True)
        else:
            log.warn(
                f'{ticker} - no data found for puts')
        # if able to merge fetch + last for today
    else:
        log.error(
            f'label={work_dict.get("label", None)} - '
            f'unsupported fetch_data('
            f'work_dict={work_dict}, '
            f'fetch_type={fetch_type}'
            f')')
        raise NotImplementedError
    # end of supported fetchers

    return status_df, df
# end of fetch_data
