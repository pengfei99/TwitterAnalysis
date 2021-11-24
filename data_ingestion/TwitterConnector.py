import logging
import tweepy as tw


class TwitterConnector:
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.logger = logging.getLogger()

    def create_api(self):
        client_auth = tw.OAuthHandler(self.consumer_key, self.consumer_secret)
        client_auth.set_access_token(self.access_token, self.access_token_secret)
        api = tw.API(client_auth, wait_on_rate_limit=True, retry_count=5, retry_delay=1)
        try:
            api.verify_credentials()
        except Exception as e:
            self.logger.error("Error during authentication")
            raise e
        self.logger.info("Authentication OK")
        return api

