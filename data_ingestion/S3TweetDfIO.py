import pyarrow.parquet as pq
import s3fs
import pyarrow as pa
from datetime import date


class S3TweetDfIO:
    def __init__(self, endpoint, access_key, access_secret, token):
        url = f"https://{endpoint}"
        self.fs = s3fs.S3FileSystem(key=access_key, secret=access_secret, token=token,
                                    client_kwargs={'endpoint_url': url})

    # This function write a pandas dataframe to s3 in parquet format
    def write_df_to_s3(self, df, bucket_name, path):
        # Convert pandas df to Arrow table
        table = pa.Table.from_pandas(df)
        file_uri = f"{bucket_name}/{path}"
        pq.write_to_dataset(table, root_path=file_uri, filesystem=self.fs)

    # This function read a parquet file and return a arrow table
    def read_parquet_from_s3(self, bucket_name, path):
        file_uri = f"{bucket_name}/{path}"
        dataset = pq.ParquetDataset(file_uri, filesystem=self.fs)
        return dataset.read().to_pandas()


def main():
    # s3 creds config
    endpoint = "changeMe"
    access_key = "changeMe"
    access_secret = "changeMe"
    token = "changeMe"
    # create an instance of the tweet io
    s3_tweet_io = S3TweetDfIO(endpoint, access_key, access_secret, token)

    # set up write path
    df = None
    bucket = "pengfei"
    current_date = date.today().strftime("%d-%m-%Y")
    output_path = f"diffusion/demo_prod/tweet_{current_date}"
    s3_tweet_io.write_df_to_s3(df, bucket, output_path)

    # set up read example
    df_read = s3_tweet_io.read_parquet_from_s3(bucket, output_path)
    df_read.head()


if __name__ == "__main__":
    main()
