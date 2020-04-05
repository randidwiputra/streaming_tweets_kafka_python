from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer


consumer_key = "BV7g00mtbPR5sx7PAFuvGct7y"
consumer_secret = "T7obwtr3e3ilqXs4JulMUiQf6HF"
accses_token = "1151662035026898944-Inx0fLd"
access_token_secret = "ZTHjvDL5p7ENWKgfgkWg"

class StdOutListener(StreamListener):
    def on_data(self, data):
        kafka_producer.send("covid19indonesia", data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print(status)


kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(accses_token, access_token_secret)

stream = Stream(auth, l)
stream.filter(track=["covid19indonesia"])
