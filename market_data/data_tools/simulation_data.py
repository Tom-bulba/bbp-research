import pandas as pd
import numpy as  np
import datetime
import json
from data_tools.aws_tools import AWSHandler

class SimulationDataConfiguration():
    def __init__(self, configuration_file_path):
        self.configuration_file_path = configuration_file_path
        self.load_json()

    def load_json(self):
        configuration_file_json = open(self.configuration_file_path)
        configuration_file = json.load(configuration_file_json)
        configuration_file_json.close()
        self.working_date = configuration_file["working_data"]
        self.env = configuration_file["env"]
        self.market_data_configuration = configuration_file["market_data"]
        self.ebs_market_data_configuration = configuration_file["ebs_market_data"]
        self.market_data_1_configuration = configuration_file["market_data_1"]
        self.merged_raw_data_configuration = configuration_file["merged_raw_data"]
        self.view_market_data_configuration = configuration_file["view_market_data"]

class SimulationDataCreator():
    """
    SimulationDataCreator create data for simulation, the process is
    - local/ec2 - upload market_data, market_data_1 and view_market_data
    - for market_data, market_data_1 and view_market_data convert the invoker
    - list the brokers in each data
    - merge market_data, market_data_1 -> list the brokers
    """
    def __init__(self, configration_file):
        self.configuration = SimulationDataConfiguration(configration_file)

    def upload_data(self, data_to_upload):
        daily_data_dict = {}
        if self.configuration.env == "local":
            if "market_data" in data_to_upload:
                daily_data_dict["market_data"] = {}
                daily_data_dict["market_data"]["data"] = pd.read_csv(self.configuration.market_data_configuration["file_path"])
            if "market_data_1" in data_to_upload:
                daily_data_dict["market_data_1"] = {}
                daily_data_dict["market_data_1"]["data"] = pd.read_csv(self.configuration.market_data_1_configuration["file_path"])
            if "view_market_data" in data_to_upload:
                daily_data_dict["view_market_data"] = {}
                daily_data_dict["view_market_data"]["data"] = pd.read_csv(self.configuration.view_market_data_configuration["file_path"])
            if "ebs_market_data" in data_to_upload:
                daily_data_dict["ebs_market_data"] = {}
                daily_data_dict["ebs_market_data"]["data"] = pd.read_csv(self.configuration.ebs_market_data_configuration["file_path"])
            if "merged_raw_data" in data_to_upload:
                daily_data_dict["merged_raw_data"] = {}
                daily_data_dict["merged_raw_data"]["data"] = pd.read_csv(self.configuration.merged_raw_data_configuration["file_path"])
        if self.configuration.env == "ec2":
            aws_handler = AWSHandler("upload daily data")
            if "market_data" in data_to_upload:
                daily_data_dict["market_data"] = {}
                daily_data_dict["market_data"]["data"] = aws_handler.get_file_from_bucket(self.configuration.working_date, "market_data.csv", "ebs-trading-arena")
            if "market_data_1" in data_to_upload:
                daily_data_dict["market_data_1"] = {}
                daily_data_dict["market_data_1"]["data"] = aws_handler.get_file_from_bucket(self.configuration.working_date, "market_data_1.csv", "ebs-trading-arena")
            if "view_market_data" in data_to_upload:
                daily_data_dict["view_market_data"] = {}
                daily_data_dict["view_market_data"]["data"] = aws_handler.get_file_from_bucket(self.configuration.working_date, "view_market_data.csv", "ebs-trading-arena")
            if "ebs_market_data" in data_to_upload:
                daily_data_dict["ebs_market_data"] = {}
                daily_data_dict["ebs_market_data"]["data"] = aws_handler.get_file_from_bucket(self.configuration.working_date, "ebs_market_data.csv", "ebs-trading-arena")
            if "merged_raw_data" in data_to_upload:
                daily_data_dict["merged_raw_data"] = {}
                daily_data_dict["merged_raw_data"]["data"] = aws_handler.get_file_from_bucket(self.configuration.working_date, "merged_raw_data.csv", "ebs-trading-arena")
        return daily_data_dict


    def create_simulation_data(self):
        pass

    def convert_invokers(self, data_dict, data_to_convert):
        for data_type_to_convert in data_to_convert:
            data_dict[data_type_to_convert]["data"], data_dict[data_type_to_convert]["brokers"] = self.convert_invoker(data_dict[data_type_to_convert]["data"], "invoker")
        return data_dict

    def convert_invoker(self, df, col_to_convert):
        """
        convert_invoker will convert into bool columns the df[col_to_convert] with a the type of bank_name is a string
        """
        # convert to string
        # df[col_to_convert] = df[col_to_convert].apply(eval)
        df[col_to_convert] = df[col_to_convert].apply(lambda x: convert_invokers_string_to_list(x)) 

        # convert into true/false columns
        unique_items = pd.Series([x for _list in df[col_to_convert] for x in _list])
        unique_items = unique_items.value_counts()
        
        # print value counts after converting
        msg = f"the value counts of column {col_to_convert} is:"
        print(msg +'\n')
        print(unique_items)
        qoutes_in_data = list(unique_items.index)
        brokers_in_data = []
        for qoute_b in qoutes_in_data:
            if "offer" in qoute_b:
                broker = qoute_b[0:-8]
                brokers_in_data.append(broker)
    
        # Create empty dict
        bool_dict = {}
    
        # Loop through all the tags
        for i, item in enumerate(unique_items.keys()):
            item_lists = df[col_to_convert]
            # Apply boolean mask that returns a True-False list of whether a tag is in a taglist
            bool_dict[item] = item_lists.apply(lambda x: item in x)
            
        # Return the results as a dataframe
        bool_df = pd.DataFrame(bool_dict)
        print(f"{bool_df.shape[0]}, {df.shape[0]}")
        df = pd.merge(df.reset_index(), bool_df.reset_index(), on='index', how="inner")
        df.drop(columns=["index"], inplace=True)
        return df, brokers_in_data

    def add_times(self, data_dict, data_to_add_time_to):
        for data_type_to_convert in data_to_add_time_to:
            data_dict[data_type_to_convert]["data"] = self.add_time(data_dict[data_type_to_convert]["data"], "timestamp")
        return data_dict

    def add_time(self, df, time_column_to_convert):
        """
        add_time add time 

        Parameters
        ----------
        columns_to_add : list, optional
            [description], by default ["all"]
        """
        df['time'] = df[[time_column_to_convert]].apply(lambda row: convert_ts_to_israel_utc(row[time_column_to_convert]), axis=1)
        df['hour'] = df['time'].dt.hour
        df['minute'] = df['time'].dt.minute
        return df
        
    def print_daily_flow(self, df, time_mesurment):
        pass


    def merge_markets(self, df1, df2, col_to_merge_on, fill_empty_cell=False):
        """
        merge_markets given the market data frame and the view market data frame the function will merged into one market data frame

        Parameters
        ----------
        df1 : DataFrame
            market df
        df2 : DataFrame
            view market df
        col_to_merge_on : string
            col name in both datafrmae to merge on
        fill_empty_cell : bool, optional
            if True, for each column ,will propagate last valid observation forward, by default False

        Returns
        -------
        DataFrame
            merged df with one invoker column
        """
        # merge both data dfs
        merged_df = pd.merge(df1, df2, on=col_to_merge_on, how='outer', suffixes=('_left', '_right'))
        # update invoker columns
        merged_df["invoker"] = merged_df.apply(merge_to_invoker_columns, axis=1)
        # drop old invoker columns
        merged_df.drop(columns=['invoker_left', 'invoker_right'], inplace=True)
        # fill data 
        if fill_empty_cell:
            merged_df.fillna(method='pad', axis='rows', inplace=True)
        return merged_df

    def keep_data_between_hours(self, df, start_hour, end_hour, hour_col_name):
        df = df[(df[hour_col_name] >= start_hour) & (df[hour_col_name] <= end_hour)]
        return df

def convert_invokers_string_to_list(string_to_convert):
    string_to_convert = string_to_convert.replace("'", "")
    string_to_convert = string_to_convert.replace(" ", "")
    string_to_convert = string_to_convert[1:-1]
    string_to_convert = string_to_convert.split(",")
    string_to_convert = list(set(string_to_convert))
    return string_to_convert



def merge_to_invoker_columns(row):
    left_invokers = row['invoker_left']
    right_invokers = row['invoker_right']
    # isinstance(tom.iloc[0]["invoker_left"], float)
    if not isinstance(left_invokers, float):
        if not isinstance(right_invokers, float):
            invoker_union = list(set.union(set(left_invokers), set(right_invokers)))
            return invoker_union
        else:
            return left_invokers
    else:
        return right_invokers


def convert_ts_to_israel_utc(ts_to_convert):
    value_to_convert = int(ts_to_convert / 1000)
    try:
        valiue_to_return = datetime.datetime.fromtimestamp(value_to_convert)
    except:
        valiue_to_return = -1.0
    return valiue_to_return

# if __name__ == "__main__":
#     simulation_data_creator = SimulationDataCreator("configuration_files/simulation_data_creator.json")
#     # upload the data
#     data_dict = simulation_data_creator.upload_data(["market_data", "market_data_1", "view_market_data"])
#     # convert invoker column
#     data_dict = simulation_data_creator.convert_invokers(data_dict, ["market_data", "market_data_1", "view_market_data"])
#     # add time
#     data_dict = simulation_data_creator.add_times(data_dict, ["market_data", "market_data_1", "view_market_data"])
#     # merge into one DataFrame
#     merged_data = simulation_data_creator.merge_markets(data_dict["market_data"]["data"], data_dict["market_data_1"]["data"], "timestamp", True)


    # market_data = pd.read_csv(configration_file["market_data"]["file_path"], nrows=100000)
    # print(market_data.info(verbose=True))
    # market_data = simulation_data_creator.convert_invoker(market_data, "invoker")
    # print(market_data.info(verbose=True))


    # market_data_1 = pd.read_csv(configration_file["market_data_1"]["file_path"], nrows=100000)
    # print(market_data_1.info(verbose=True))
    # market_data_1 = simulation_data_creator.convert_invoker(market_data_1, "invoker")
    # print(market_data_1.info(verbose=True))
    
    # # merge market data
    # merged_df = simulation_data_creator.merge_markets(market_data, market_data_1, "timestamp")



    # market_data = simulation_data_creator.add_time(market_data, "timestamp")
    # print(market_data.info(verbose=True))