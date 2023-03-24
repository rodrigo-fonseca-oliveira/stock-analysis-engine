"""
Custom BollingerBands

https://www.investopedia.com/terms/b/bollingerbands.asp

Overlap

**Supported environment variables**

::

    # to show debug, trace logging please export ``SHARED_LOG_CFG``
    # to a debug logger json file. To turn on debugging for this
    # library, you can export this variable to the repo's
    # included file with the command:
    export SHARED_LOG_CFG=/opt/sa/analysis_engine/log/debug-logging.json
"""

import analysis_engine.ae_talib as ae_talib
import analysis_engine.consts as ae_consts
import analysis_engine.indicators.base_indicator as base_indicator


class IndicatorBollingerBands(base_indicator.BaseIndicator):
    """IndicatorBollingerBands"""

    def __init__(
            self,
            **kwargs):
        """__init__

        Custom indicator example for showing a
        ``Bollinger Band``
        within an algo for analyzing intraday minute datasets

        Please refer to the `analysis_engine.indicators.base_indicator.Ba
        seIndicator source code for the latest supported parameters <ht
        tps://github.com/AlgoTraders/stock-analysis-engine
        /blob/master/
        analysis_engine/indicators/base_indicator.py>`__

        :param kwargs: keyword arguments
        """
        super().__init__(**kwargs)
    # end of __init__

    def get_configurables(
            self,
            **kwargs):
        """get_configurables

        helper for setting up algorithm configs for this indicator
        and programmatically set the values based off the domain
        rules

        .. code-block:: python

            from analysis_engine.indicators.bollinger_bands \
                import IndicatorBollingerBands
            ind = IndicatorBollingerBands(config_dict={
                    'verbose': True
                }).get_configurables()

        :param kwargs: keyword args dictionary
        """
        self.ind_confs = []

        # common:
        self.build_base_configurables(
            ind_type='overlap',
            category='technical',
            uses_data=self.config.get(
                'uses_data',
                'minute'),
            version=1)

        # custom:
        self.ind_confs.append(self.build_configurable_node(
            name='num_points',
            conf_type='int',
            max_value=200,
            min_value=2,
            default_value=20,
            inc_interval=1))
        self.ind_confs.append(self.build_configurable_node(
            name='buy_below_percent',
            conf_type='percent',
            min_value=1.0,
            max_value=100.0,
            default_value=1.0,
            inc_interval=3.0))
        self.ind_confs.append(self.build_configurable_node(
            name='sell_above_percent',
            conf_type='percent',
            min_value=1.0,
            max_value=100.0,
            default_value=1.0,
            inc_interval=3.0))
        self.ind_confs.append(self.build_configurable_node(
            name='upper_stdev',
            conf_type='float',
            min_value=0.5,
            max_value=10.0,
            default_value=0.5,
            inc_interval=1.0))
        self.ind_confs.append(self.build_configurable_node(
            name='lower_stdev',
            conf_type='float',
            min_value=0.5,
            max_value=10.0,
            default_value=0.5,
            inc_interval=1.0))
        self.ind_confs.append(self.build_configurable_node(
            name='matype',
            conf_type='int',
            min_value=0,
            max_value=0,
            default_value=0,
            inc_interval=1))

        # output / reporting:
        self.ind_confs.append(self.build_configurable_node(
            name='upperband',
            conf_type='int',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='middleband',
            conf_type='float',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='lowerband',
            conf_type='float',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='upper_lower_width',
            conf_type='float',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='amount_to_low',
            conf_type='float',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='amount_to_high',
            conf_type='float',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='percent_to_low',
            conf_type='percent',
            is_output_only=True))
        self.ind_confs.append(self.build_configurable_node(
            name='percent_to_high',
            conf_type='percent',
            is_output_only=True))

        default_values_dict = {}
        for node in self.ind_confs:
            name = node['name']
            default_value = node.get(
                'default',
                None)
            default_values_dict[name] = default_value

        use_file = None
        try:
            if __file__:
                use_file = __file__
        except Exception:
            use_file = None

        self.starter_dict = {
            'name': self.__class__.__name__.lower().replace(
                'indicator',
                ''),
            'module_path': use_file,
            'category': default_values_dict.get(
                'category',
                'momentum'),
            'type': default_values_dict.get(
                'type',
                'technical'),
            'uses_data': default_values_dict.get(
                'uses_data',
                'minute'),
            'verbose': default_values_dict.get(
                'verbose',
                False)
        }
        self.starter_dict.update(default_values_dict)

        self.lg(
            f'configurables={ae_consts.ppj(self.ind_confs)} for '
            f'class={self.__class__.__name__} in file={use_file} '
            f'starter:\n {ae_consts.ppj(self.starter_dict)}')

        return self.ind_confs
    # end of get_configurables

    def get_starter_dict(
            self):
        if not self.starter_dict:
            self.get_configurables()
        return self.starter_dict
    # end of get_starter_dict

    def process(
            self,
            algo_id,
            ticker,
            dataset):
        """process

        Derive custom indicator processing to determine buy and sell
        conditions before placing orders. Just implement your own
        ``process`` method.

        Please refer to the TA Lib guides for details on building indicators:

        - Overlap Studies
          https://mrjbq7.github.io/ta-lib/func_groups/overlap_studies.html
        - Momentum Indicators
          https://mrjbq7.github.io/ta-lib/func_groups/momentum_indicators.html
        - Volume Indicators
          https://mrjbq7.github.io/ta-lib/func_groups/volume_indicators.html
        - Volatility Indicators
          https://mrjbq7.github.io/ta-lib/func_groups/volatility_indicators.html
        - Price Transform
          https://mrjbq7.github.io/ta-lib/func_groups/price_transform.html
        - Cycle Indicators
          https://mrjbq7.github.io/ta-lib/func_groups/cycle_indicators.html
        - Pattern Recognition
          https://mrjbq7.github.io/ta-lib/func_groups/pattern_recognition.html
        - Statistic Functions
          https://mrjbq7.github.io/ta-lib/func_groups/statistic_functions.html
        - Math Transform
          https://mrjbq7.github.io/ta-lib/func_groups/math_transform.html
        - Math Operators
          https://mrjbq7.github.io/ta-lib/func_groups/math_operators.html

        :param algo_id: string - algo identifier label for debugging datasets
            during specific dates
        :param ticker: string - ticker
        :param dataset: dictionary of ``pandas.DataFrame(s)`` to process
        """

        # set the algo config indicator 'uses_data' to 'day' or 'minute'
        df_status, self.use_df = self.get_subscribed_dataset(
            dataset=dataset)

        if df_status == ae_consts.EMPTY:
            self.lg('process end - no data found')
            return

        # notice the self.num_points is now a member variable
        # because the BaseIndicator class's __init__
        # converts any self.config keys into useable
        # member variables automatically in your derived class
        self.lg(
            f'process - num_points={self.num_points} '
            f'df={len(self.use_df.index)}')
        """
        upperband, middleband, lowerband = BBANDS(
            close,
            timeperiod=5,
            nbdevup=2,
            nbdevdn=2,
            matype=0)
        """
        num_records = len(self.use_df.index)
        if num_records > self.num_points:
            cur_value = self.use_df['close'].iloc[-1]
            first_date = self.use_df['date'].iloc[0]
            end_date = self.use_df['date'].iloc[-1]
            start_row = num_records - self.num_points
            self.use_df = self.use_df[start_row:num_records]
            """
            for idx, row in self.use_df[start_row:-1].iterrows():
                high = row['high']
                low = row['low']
                open_val = row['open']
                close = row['close']
                row_date = row['date']
                self.lg(
                    f'{row_date} - high={high}, low={low}, '
                    f'close={close}, period={self.num_points}')
            """
            closes = self.use_df['close'].values

            (upperbands,
             middlebands,
             lowerbands) = ae_talib.BBANDS(
                 close=closes,
                 timeperiod=self.num_points,
                 nbdevup=self.upper_stdev,
                 nbdevdn=self.lower_stdev,
                 matype=self.matype)

            """
            Determine a buy or a sell as a label
            """

            self.upperband = ae_consts.to_f(upperbands[-1])
            self.middleband = ae_consts.to_f(middlebands[-1])
            self.lowerband = ae_consts.to_f(lowerbands[-1])

            if cur_value <= 0:
                self.lg(f'invalid current_value={cur_value}')
                return

            self.amount_to_low = ae_consts.to_f(cur_value - self.lowerband)
            self.amount_to_high = ae_consts.to_f(self.upperband - cur_value)

            if self.amount_to_low < 0:
                self.percent_to_low = -1 * ae_consts.to_f(
                    self.amount_to_low / cur_value * 100.0)
            else:
                self.percent_to_low = ae_consts.to_f(
                    self.amount_to_low / cur_value * 100.0)

            if self.amount_to_high < 0:
                self.percent_to_high = -1 * ae_consts.to_f(
                    self.amount_to_high / cur_value * 100.0)
            else:
                self.percent_to_high = ae_consts.to_f(
                    self.amount_to_high / cur_value * 100.0)

            self.is_buy = ae_consts.INDICATOR_IGNORE
            self.is_sell = ae_consts.INDICATOR_IGNORE

            if self.percent_to_low > self.buy_below_percent:
                self.is_buy = ae_consts.INDICATOR_BUY
            elif self.percent_to_high > self.sell_above_percent:
                self.is_sell = ae_consts.INDICATOR_SELL

            self.lg(
                f'process end - {first_date} to {end_date} '
                f'buy_below={self.buy_below_percent} is_buy={self.is_buy} '
                f'sell_above={self.sell_above_percent} is_sell={self.is_sell}')
        else:
            self.lg('process end')
    # end of process

    def reset_internals(
            self):
        """reset_internals"""
        self.is_buy = ae_consts.INDICATOR_RESET
        self.is_sell = ae_consts.INDICATOR_RESET
    # end of reset_internals

# end of IndicatorBollingerBands


def get_indicator(
        **kwargs):
    """get_indicator

    Make sure to define the ``get_indicator`` for your custom
    algorithms to work as a backup with the ``sa.py`` tool...
    Not anticipating issues, but if we do with importlib
    this is the backup plan.

    Please file an issue if you see something weird and would like
    some help:
    https://github.com/AlgoTraders/stock-analysis-engine/issues

    :param kwargs: dictionary of keyword arguments
    """
    print('getting indicator')
    return IndicatorBollingerBands(**kwargs)
# end of get_indicator
