#!/usr/bin/env python
# coding: utf-8
"""
Created on April 26 2024

This script contains a function to truncate repeated occurrences of smart activity, which are assumed to be one of the prime examples of auto-generated activities being triggered that doesn't truly correspond to real user activity.

Updated June 7 2024
* Uses ACCESS_TIME instead of ACCESS_INSTANT

@author: Seunghwan (Nigel) Kim
@email: seunghwan.kim@wustl.edu
"""

import pandas as pd

import pandas as pd

class AuditLogCleaner:
    def __init__(self, df, timestamp_col, event_type_col, cap_gap_minutes=5):
        """
        Initialize the SmartActionProcessor with a DataFrame and column names.

        IMPORTANT NOTE:
        These truncation should always happen AFTER you assigned the session_IDs to the raw  audit logs.
        This is because this function will remove rows from the raw audit logs and may potentially introduce "ghost gaps" > 5 minutes that may introduce false session split points.
        Although rare, this will happen when the consecutive "smart" action sequence is long enough and spans a time period > 5 minutes.

        Parameters:
        df (pd.DataFrame): The DataFrame containing audit logs.
        timestamp_col (str): The column name for timestamps.
        event_type_col (str): The column name for action names.
        cap_gap_minutes (int): The maximum gap in minutes to consider for truncating actions. Default is 5 minutes.
        """
        self.df = df
        self.timestamp_col = timestamp_col
        self.event_type_col = event_type_col
        self.SESSION_INTERVAL = cap_gap_minutes * 60


    def truncate_smart_actions(self):
        '''
        Truncate consecutive smart actions to a single action if they occur within a short time frame.
        METRIC_IDs 20030-"SmartLink used" and 20040-"SmartText used", are common examples of auto-generated "smart" actions.
        We define a rule: “if there are multiple consecutive smart activity metric (e.g., 20030 SmartLink used, or 20040 SmartText used),
        for the same patient over a period of time (scale of seconds to minutes), then we just truncate that to a single smart action”
        Returns:
            pd.DataFrame: The DataFrame with truncated smart actions.
        '''
        # self.df['PAT_ID'].fillna('', inplace=True)
        self.df.fillna({'PAT_ID':''}, inplace=True)

        self.df['smart_action_to_remove'] = 0
        self.df.loc[
            (self.df['PAT_ID'] == self.df['PAT_ID'].shift(1)) &
            (self.df[self.timestamp_col] - self.df[self.timestamp_col].shift(1) <= self.SESSION_INTERVAL) &
            (self.df[self.event_type_col].isin([20030, 20040])) &
            (self.df[self.event_type_col].shift(1).isin([20030, 20040])),
            'smart_action_to_remove'
        ] = 1

        self.df = self.df[self.df['smart_action_to_remove'] != 1]
        self.df.drop(columns=['smart_action_to_remove'], inplace=True)

        # print(f"Count of audit log action events: {len(df)}")
        # print(f"Unique count of users: {df.USER_ID.nunique()}")
        # print(f"Unique count of patients: {df.PAT_ID.nunique()}")
        # print(f"Unique count of patient encounters: {df.CSN.nunique()}")

        return self.df

    def remove_auto_gen(self, remove_same_actions=True):
        """
        Remove auto-generated actions that have the same timestamp. Keep first.

        Parameters:
        remove_same_actions (bool): Whether to remove actions with the same name. Default is True.

        Returns:
        pd.DataFrame: The DataFrame with auto-generated actions removed.
        """
        # self.df['PAT_ID'].fillna('', inplace=True)
        self.df.fillna({'PAT_ID': ''}, inplace=True)

        self.df['remove'] = 0
        if remove_same_actions:
            self.df.loc[
                (self.df['PAT_ID'] == self.df['PAT_ID'].shift(1)) &
                (self.df[self.timestamp_col] - self.df[self.timestamp_col].shift(1) == 0) &
                (self.df[self.event_type_col] == self.df[self.event_type_col].shift(1)),
                'remove'
            ] = 1
        else:
            self.df.loc[
                (self.df['PAT_ID'] == self.df['PAT_ID'].shift(1)) &
                (self.df[self.timestamp_col] - self.df[self.timestamp_col].shift(1) == 0),
                'remove'
            ] = 1

        self.df = self.df[self.df['remove'] != 1]
        self.df.drop(columns=['remove'], inplace=True)

        return self.df