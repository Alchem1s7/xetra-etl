import pandas as pd
import boto3
from io import StringIO, BytesIO
from dotenv import load_dotenv
import os
import datetime
from datetime import date
from botocore.exceptions import NoCredentialsError
load_dotenv()

# Set variables
input_date = "2022-01-01"
end_date = "2022-02-28"
source_bucket_name = "xetra-1234"
target_bucket_name = "xetra-project-daniel"
columns_of_interest = [
    "ISIN","Date","Time","StartPrice","MaxPrice",
    "MinPrice","EndPrice","TradedVolume"
]

try:
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise NoCredentialsError

except NoCredentialsError:
    print("Credentials unavailables")


# Define functions

def workflow():

    s3_session = connection_to_s3(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    all_objects_list, source_bucket = get_objects_from_s3(
        input_date=input_date,
        end_date=end_date,
        s3_session=s3_session,
        bucket_name=source_bucket_name
    )

    df = consolidate_df(
        all_objects_list=all_objects_list, 
        source_bucket=source_bucket, 
        columns_of_interest=columns_of_interest
    )

    df = new_columns_and_transformations(df=df)

    write_df_to_s3(
        df=df, 
        s3_session=s3_session, 
        target_bucket_name=target_bucket_name
    )


def connection_to_s3(aws_access_key_id, aws_secret_access_key):
    
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_session = session.resource('s3')
    return s3_session
    

def get_objects_from_s3(
        input_date,
        end_date,
        s3_session,
        bucket_name
    ):
    
    source_bucket = s3_session.Bucket(bucket_name)

    date_list = pd.date_range(input_date, end_date)
    date_strings = [date.strftime('%Y-%m-%d') for date in date_list]

    all_objects_list = []
    
    for date in date_strings:
        bucket_obj = source_bucket.objects.filter(Prefix=date)
        objects = [obj for obj in bucket_obj]
        all_objects_list = all_objects_list + objects
    
    return all_objects_list, source_bucket


def consolidate_df(all_objects_list, source_bucket, columns_of_interest):

    dfs_list = []

    for obj in all_objects_list:

        csv_obj = source_bucket.Object(key=obj.key).get().get("Body").read().decode("utf8")
        data = StringIO(csv_obj)
        
        if len(csv_obj.split("\n")) > 1:

            df = pd.read_csv(data, sep=",")
            dfs_list.append(df)

    df = pd.concat(dfs_list, ignore_index=True)
    df = df.loc[:,columns_of_interest].copy()
    
    return df


def new_columns_and_transformations(df):
    
    df["opening_price"] = pd.NA
    df["opening_price"] = pd.NA
    df["prev_closing_price"] = pd.NA

    df.loc[:,"opening_price"] = df.sort_values(by="Time").groupby(["ISIN","Date"])["StartPrice"].transform("first")
    df.loc[:,"closing_price"] = df.sort_values(by="Time").groupby(["ISIN","Date"])["StartPrice"].transform("last")

    df_grouped = (
        df
        .groupby(["ISIN","Date"], as_index=False)
        .agg(
            opening_price_eur=("opening_price","min"), # It doesn't matter if we select min of max
            closing_price_eur=("closing_price","max"), # It doesn't matter if we select min of max
            minimum_price_eur=("MinPrice","min"),
            maximum_price_eur=("MaxPrice","max"),
            daily_traded_volume=("TradedVolume","sum")
        )   
    )

    df_grouped["prev_closing_price"] = df_grouped.sort_values(by="Date").groupby("ISIN")["closing_price_eur"].shift(1)
    df_grouped["change_prev_closing_%"] = 100*(df_grouped["closing_price_eur"] - df_grouped["prev_closing_price"])/df_grouped["prev_closing_price"]
    
    df_grouped.drop(columns=["prev_closing_price"], inplace=True)

    return df_grouped


def write_df_to_s3(df, s3_session, target_bucket_name):
    
    out_buffer = BytesIO()

    df.to_parquet(out_buffer, index=False)
    bucket_target = s3_session.Bucket(target_bucket_name)
    
    bucket_target.put_object(
        Body=out_buffer.getvalue(), 
        Key="xetra_daily_report.parquet"
    )
    return True


if __name__ == "__main__":
    workflow()
    print("All done")