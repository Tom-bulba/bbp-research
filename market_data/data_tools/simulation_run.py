import pandas as pd
import numpy as  np
import datetime
import json
from data_tools.aws_tools import AWSHandler


class RunSimulation():
    def __init__(self, simulation_name, hit_brokers, view_brokers, enter_conditions, exit_conditions, positon_fee, max_time_in_position):
        self.simulation_name = simulation_name
        self.hit_brokers = hit_brokers
        self.view_brokers = view_brokers
        self.enter_conditions = enter_conditions
        self.exit_conditions = exit_conditions
        self.positon_fee = positon_fee
        self.max_time_in_position = max_time_in_position
    
    def initialize_summary_df(self, simulation_type):
        if simulation_type == "increase":
            self.signal_value = []

            self.timestamp_at_entering = []
            self.hour_at_entering = []
            self.best_offer_at_entering = []
            self.best_offer_broker_at_entering = []
            self.best_bid_at_entering = []
            self.best_bid_broker_at_entering = []

            self.best_offer_at_exiting = []
            self.best_offer_broker_at_exiting = []
            self.best_bid_at_exiting = []
            self.best_bid_broker_at_exiting = []
            self.timestamp_at_exiting = []

            self.position_outcome = []
            self.position_profit = []


    # def populate_summary_df(self, simulation_type, position_data):
    #     pass


    def prepaer_data_for_simulation(self, df):
        # union self.hit_brokers with self.view_brokers
        # brokers_to_keep = list(set(self.hit_brokers).union(set(self.view_brokers)))
        # df_to_return = df.copy()
        # # keep only rows in which brokers to keep invoke
        # columns_to_iterate = []
        # for broker_to_keep in brokers_to_keep:
        #     broker_col = f"{broker_to_keep}_bid_0"
        #     columns_to_iterate.append(broker_col)
        #     broker_col = f"{broker_to_keep}_offer_0"
        #     columns_to_iterate.append(broker_col)
        
        # # keep only hit and view brokers
        # df_to_return = df_to_return[df_to_return[columns_to_iterate].any(axis=1)]

        # # drop unwanted columns
        # columns_to_drop = []

        # create offer hit columns
        offer_hit_column_name = []
        for broker_name in self.hit_brokers:
            col_name_to_append = f"{broker_name}_offer_0_rate"
            offer_hit_column_name.append(col_name_to_append)
        
        bid_hit_column_name = []
        for broker_name in self.hit_brokers:
            col_name_to_append = f"{broker_name}_bid_0_rate"
            bid_hit_column_name.append(col_name_to_append)

        # calculate best offer/bid for hit brokers
        df['best_offer_hit_rate'] = df[offer_hit_column_name].replace({0 : np.NaN, 0.0: np.NaN}).min(axis=1, skipna=True)
        df['best_offer_hit_broker'] = df[offer_hit_column_name].replace({0 : np.NaN, 0.0: np.NaN}).idxmin(axis=1, skipna=True)
        df['best_bid_hit_rate'] = df[bid_hit_column_name].replace({0 : np.NaN, 0.0: np.NaN}).max(axis=1, skipna=True)
        df['best_bid_hit_broker'] = df[bid_hit_column_name].replace({0 : np.NaN, 0.0: np.NaN}).idxmax(axis=1, skipna=True)

        # convert to numpy
        self.df_col_list = list(df.columns.values)
        df_np = df.to_numpy().copy()
        number_of_rows = df.shape[0] - 1
        return self.df_col_list, df_np, number_of_rows
    
    def spread_tag_inc_entering_condition(self, current_market_data, offer_broker, bid_broker, threshold_value):
        invoker_bid_column_name = f"{bid_broker}_bid_0"
        if current_market_data[self.df_col_list.index(invoker_bid_column_name)]:
            bid_broker_rate_name = f"{bid_broker}_bid_0_rate"
            offer_broker_rate_name = f"{offer_broker}_offer_0_rate"
            spread_tag_inc = current_market_data[self.df_col_list.index(offer_broker_rate_name)] - current_market_data[self.df_col_list.index(bid_broker_rate_name)]
            if spread_tag_inc <= threshold_value:
                is_there_signal = True
            else:
                is_there_signal = False
                spread_tag_inc = None
        else:
            is_there_signal = False
            spread_tag_inc = None
        return is_there_signal, spread_tag_inc

    def find_bests_for_hit_brokers(self, current_market_data):
        best_offer = current_market_data[self.df_col_list.index("best_offer_hit_rate")]
        best_offer_broker = current_market_data[self.df_col_list.index("best_offer_hit_broker")]
        best_bid = current_market_data[self.df_col_list.index("best_bid_hit_rate")]
        best_bid_broker = current_market_data[self.df_col_list.index("best_bid_hit_broker")]
        return best_offer, best_offer_broker, best_bid, best_bid_broker

    def check_exit_limit_condition_inc(self, current_market_data, upper_limit_value, lower_limit_value):
        exit_position_status = False
        if current_market_data[self.df_col_list.index("best_bid_hit_rate")] >= upper_limit_value or current_market_data[self.df_col_list.index("best_bid_hit_rate")] < lower_limit_value:
            exit_position_status = True
        return exit_position_status
    
    def documnet_position(self, market_data_at_entering, market_data_at_exiting, signal_value):
        self.timestamp_at_entering.append(market_data_at_entering[self.df_col_list.index("timestamp")])
        self.signal_value.append(signal_value)

        self.hour_at_entering.append(market_data_at_entering[self.df_col_list.index("hour")])
        self.best_offer_at_entering.append(market_data_at_entering[self.df_col_list.index("best_offer_hit_rate")])
        self.best_offer_broker_at_entering.append(market_data_at_entering[self.df_col_list.index("best_offer_hit_broker")])
        self.best_bid_at_entering.append(market_data_at_entering[self.df_col_list.index("best_bid_hit_rate")])
        self.best_bid_broker_at_entering.append(market_data_at_entering[self.df_col_list.index("best_bid_hit_broker")])

        self.best_offer_at_exiting.append(market_data_at_exiting[self.df_col_list.index("best_offer_hit_rate")])
        self.best_offer_broker_at_exiting.append(market_data_at_exiting[self.df_col_list.index("best_offer_hit_broker")])
        self.best_bid_at_exiting.append(market_data_at_exiting[self.df_col_list.index("best_bid_hit_rate")])
        self.best_bid_broker_at_exiting.append(market_data_at_exiting[self.df_col_list.index("best_bid_hit_broker")])
        self.timestamp_at_exiting.append(market_data_at_exiting[self.df_col_list.index("timestamp")])

        position_outcome = market_data_at_exiting[self.df_col_list.index("best_bid_hit_rate")] - market_data_at_entering[self.df_col_list.index("best_offer_hit_rate")]

        self.position_outcome.append(np.round(position_outcome, 6))
        self.position_profit.append(np.round(position_outcome - self.positon_fee, 6))
    
    def create_simulation_summary(self):
        summary_dict = {
            "signal_value": self.signal_value,
            "timestamp_at_entering": self.timestamp_at_entering,
            "hour_at_entering": self.hour_at_entering,
            "best_offer_at_entering": self.best_offer_at_entering,
            "best_offer_broker_at_entering": self.best_offer_broker_at_entering,
            "best_bid_at_entering": self.best_bid_at_entering,
            "best_bid_broker_at_entering": self.best_bid_broker_at_entering,
            "best_offer_at_exiting": self.best_offer_at_exiting,
            "best_offer_broker_at_exiting": self.best_offer_broker_at_exiting,
            "best_bid_at_exiting": self.best_bid_at_exiting,
            "best_bid_broker_at_exiting": self.best_bid_broker_at_exiting,
            "timestamp_at_exiting": self.timestamp_at_exiting,
            "position_outcome": self.position_outcome,
            "position_profit": self.position_profit
        }
        summary_df = pd.DataFrame.from_dict(summary_dict)
        return summary_df

