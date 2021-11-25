import logging
import tweepy as tw
import pandas as pd


class TwitterConnector:
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        self.client_api = self.create_api(consumer_key, consumer_secret, access_token, access_token_secret)

    @staticmethod
    def create_api(consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        client_auth = tw.OAuthHandler(consumer_key, consumer_secret)
        client_auth.set_access_token(access_token, access_token_secret)
        api = tw.API(client_auth, wait_on_rate_limit=True, retry_count=5, retry_delay=1)
        try:
            api.verify_credentials()
        except Exception as e:
            logging.error("Error during authentication")
            raise e
        logging.info("Authentication OK")
        return api

    def get_tweets(self, search_words: str, lang: str, result_type: str, max_tweet_count: int):
        return self.client_api.search_tweets(q=search_words, lang=lang, result_type=result_type, count=max_tweet_count)

    @staticmethod
    def generate_tweet_df(tweets):
        # init dataframe
        df = pd.DataFrame(columns=['name', 'date', 'text'])
        index = 0
        for tweet in tweets:
            # get column value for each tweet
            tweet_dict = tweet._json
            # add new row to the dataframe
            df.loc[index] = pd.Series({'name': tweet_dict.get("user").get("name"), 'date': tweet_dict.get("created_at"),
                                       'text': tweet_dict.get("text")})
            index = index + 1
        return df


def main():
    consumer_key = "changeMe"
    consumer_secret = "changeMe"
    access_token = "changeMe"
    access_token_secret = "changeMe"
    # create an instance of twitter connector
    tc = TwitterConnector(consumer_key, consumer_secret, access_token, access_token_secret)
    # filter the search result by using below key words
    search_words = "#insee"
    # We can get tweet before certain date
    # until_date = "2021-11-24
    # specify the
    language = "fr"
    # the max tweet number will be retained in the result
    max_tweet_count = 1000000
    result_type = "mixed"
    # get the tweets
    tweets = tc.get_tweets(search_words, language, result_type, max_tweet_count)
    # generate a pandas df from the tweet
    df = tc.generate_tweet_df(tweets)


if __name__ == "__main__":
    main()
