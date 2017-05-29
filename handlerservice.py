from flask import Flask
import tweepy
import facebook
import time
app = Flask(__name__)

cfg = { 
		    "consumer_key"        : "047XaKHyngp0vE3r6xMnQeFQC",
		    "consumer_secret"     : "FU7eRdXnSAk3Ti7wFkr7Cm33cASKcvwEAeWr0CY4CjOKuzzHgx",
		    "access_token"        : "779672488590573568-LjwknNSOfZ37OGGuz40Zx9z7OcPGwbK",
		    "access_token_secret" : "2LuKr4D17V9rqTzcQVYKtGRogHkEreie3BEdkGfMIwSfZ" 
		    }
@app.route('/social_media/handler/<string:post>/<int:token>', methods=['GET'])
def call_handler(token, post):
	if token==1:
		  # Fill in the values noted in previous step here
		  
		  auth = tweepy.OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
  		  auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
		  api = tweepy.API(auth)
		  '''for status in tweepy.Cursor(api.user_timeline).items():
		    try:
			api.destroy_status(status.id)
		    except:
			pass''' 
		  	
		  #tweet = "Hello hi"
		  #tweet2= "abcdefg"
		  status = api.update_status(status=post)
		  #status= api.update_status(status=tweet2)
		  # Yes, tweet is called 'status' rather confusing
		  time.sleep(5)
		  return 'Posted on twitter'
	elif token==2:
		#the given token is long lived and is valid for 2 months.
		graph = facebook.GraphAPI('EAACEdEose0cBAOz10ZA8On4tZB9ZC3GTw4DovkJV9d6ZAx9qpzU8RtiOfmNZC5fR3yOxCy5zo79QX4crpkJZAE7e9lNNsnqKdJDNGbWyu8O7ZAfbAhHm9rwZCZBW5mKb66c8rV8RzX9QoXfIRPnAlMFTZBkMpQoz4N4uZBZAVfbwl5zYJdavPxMVLFVy0qngnufZCCrEZD')
		graph.put_object("238891636527403", "feed", message=post)
		return 'Posted on facebook'

