####################################################################################################
##################     Predict 452 - Team Sports and Recreation Project             ################
################## By: Jill Wieck, Marty Hand, Scott Kennedy, Maksim Yuster         ################
####################################################################################################
################## Code Dates:                                                      ################
################## 2017-05-08 - Initial scraping program written                    ################
################## 2017-05-10 - Added Code to extract data from Twitter for for     ################
##################              specific brands and wrote them off to seperate      ################
##################              DataFrame objects. Saved these objects to csv files ################
####################################################################################################

#Loading required libraries:
from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import re
import time
import tweepy
import json

#############################################################################################
############################# Extracting Data from Callawaygolf.com #########################
#############################################################################################

#Necessary Fields to work with for the below scripts:
url = "http://www.callawaygolf.com/golf-clubs/?sz=12&start="
n = list(np.arange(0,108,12))
n = [str(i) for i in n]
end = "&format=page-element"
url_root = 'http://www.callawaygolf.com'


#Creating two lists that contain the product name and the link to the product page:
product_links = []
product_names = []
for j in n:
    url_full = url+j+end
    print(url_full)
    source_code = requests.get(url_full)
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'html.parser')

    for i in soup.find_all({'h3', ("name-link tileName")}):
        link = i.get('href')
        name = i.get("title")
        product_links.append(link)
        product_names.append(name)
        print(name,link)
    time.sleep(5)


#Deleting empty data from lists:
del product_links[87:95]
del product_names[87:95]
len(product_links)
len(product_names)


#Combining all product links with the root url:
product_links_full = []
for i in product_links:
    product_links_full.append(url_root+i)


#Creating script to call each product link and get the hashtag if available:
#If it's not available, then add space value to list product_hashtags:
find_var = re.compile('var feedId = (.*);')
product_hashtags = []

for product_page in product_links_full[13:98]:
    source_code = requests.get(product_page)
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'lxml')

    for data in soup.find_all('script'):
        for script in data:

            if find_var.search(script.string):
                line = find_var.search(script.string)
                line_hash = "#" + (line.group(1).split('"')[3]).split("-")[1]
                product_hashtags.append(line_hash)

    if "#" not in line_hash:
        product_hashtags.append(" ")
    line = ""
    line_hash = []
    time.sleep(3)


#Creating DataFrame with Product Name, Hashtag and Page Link:
callaway_df = pd.DataFrame({"Product_Name": product_names, "HashTag": product_hashtags, "Page_Link": product_links_full})
callaway_df = callaway_df[["Product_Name","HashTag","Page_Link"]]
callaway_df.to_csv("callaway_df.csv")
unique_hastags = list(set(product_hashtags))
del unique_hastags[2]


#############################################################################################
################################### Twitter API Calls #######################################
#############################################################################################
#Grabbing credentials file instead of hardcoding the keys/tokens:
with open("tw.json") as input_file:
    tw=json.load(input_file)

#Creating a connetion to the Twitter API through Tweepy package:
auth = tweepy.OAuthHandler(tw["key"], tw["secret"])
auth.set_access_token(tw["token"], tw["token_secret"])
#client = tweepy.API(auth)
client = tweepy.API(auth, retry_count=3, retry_delay=5, retry_errors=set([401, 404, 500, 503]),
                    wait_on_rate_limit=True)



######################Testing - Extracting Twitter data for each hashtag for Calaway###############################
tweet_hash = []
tweet_data = []
query = "callaway OR callawaygolf OR calawaygolf OR @callawaygolf OR @calawaygolf"
max_tweets = 10000

for hashtag in unique_hastags:
    c_query = hashtag+" "+ query
    searched_tweets = [status for status in tweepy.Cursor(client.search, q=c_query).items(max_tweets)]
    for tweet in searched_tweets:
        tweet_hash.append(hashtag)
        tweet_data.append(tweet._json)
    time.sleep(61)
#########################################################################################################





######################## Running Queries against Twitter for each brand ##################################
callaway_query = "callaway OR callawaygolf OR calawaygolf OR @callawaygolf OR @calawaygolf"
callaway_tweets = [status for status in tweepy.Cursor(client.search, q=callaway_query).items(max_tweets)]

titleist_query = "Titleist OR @Titleist OR #Titleist OR TitleistGolf"
titleist_tweets = [status for status in tweepy.Cursor(client.search, q=titleist_query).items(max_tweets)]

taylormade_query = "TaylorMade OR @TaylorMade OR #TaylorMade OR TaylorMadeGolf OR #TaylorMadeGolf"
taylormade_tweets = [status for status in tweepy.Cursor(client.search, q=taylormade_query).items(max_tweets)]

cobra_query = "Cobra Golf OR @CobraGolf OR #CobraGolf OR CobraGolf OR CobraClubs OR Club OR Clubs"
cobra_tweets = [status for status in tweepy.Cursor(client.search, q=cobra_query).items(max_tweets)]

ping_query = "Ping Golf OR @PingGolf OR #PingGolf OR PingGolf OR PingGolf OR Club OR Clubs"
ping_tweets = [status for status in tweepy.Cursor(client.search, q=ping_query).items(max_tweets)]

underarmour_query = "UnderArmour Golf OR @UnderArmourGolf OR @Golf OR #Golf OR #UnderArmourGolf"
underarmour_tweets = [status for status in tweepy.Cursor(client.search, q=underarmour_query).items(max_tweets)]






################# Writing data to json files and saving them off to working directory ##########################
def extract_json_to_list(tweets_json_list,new_list):
    for i in tweets_json_list:
        new_list.append(i._json)

def write_json_file(json_object,file_name):
    with open(file_name+".json", "w") as output:
        json.dump(json_object,output)

all_callaway_tweets = []
all_titleist_tweets = []
all_taylormade_tweets = []
all_cobra_tweets = []
all_ping_tweets = []
all_underarmour_tweets = []
extract_json_to_list(callaway_tweets,all_callaway_tweets)
extract_json_to_list(titleist_tweets,all_titleist_tweets)
extract_json_to_list(taylormade_tweets,all_taylormade_tweets)
extract_json_to_list(cobra_tweets,all_cobra_tweets)
extract_json_to_list(ping_tweets,all_ping_tweets)
extract_json_to_list(underarmour_tweets,all_underarmour_tweets)
write_json_file(all_callaway_tweets,"all_callaway_tweets")
write_json_file(all_titleist_tweets,"all_titleist_tweets")
write_json_file(all_taylormade_tweets,"all_taylormade_tweets")
write_json_file(all_cobra_tweets,"all_cobra_tweets")
write_json_file(all_ping_tweets,"all_ping_tweets")
write_json_file(all_underarmour_tweets,"all_underarmour_tweets")




############################### Extracing Specific Tweets Data #################################################
#Function to extract specific details from each tweet for each full tweet files:
def df_of_relevant_tweet_attributes(data):
    user_id = []          #Unique User ID of Tweeter
    user_name = []        #User Name of Tweeter
    tweet_txt = []        #Tweet Text
    tweet_dt = []         #Tweet Date
    tweet_loc = []        #Location of Tweet
    tweet_lang = []       #Language of Tweet
    screen_name = []      #Screen Name of Tweeter
    followers_count = []  #Count of followers of user_id
    retweet_count = []    #Count of retweets
    retweeted = []        #Indicates if Tweet was retweeted by the authenticating user.
    replied_id = []       #If someone replied to ones tweet
    for tweet in data:
        if 'text' in tweet:
            user_id.append(tweet['user']['id'])
            user_name.append(tweet['user']['name'])
            tweet_dt.append(tweet['created_at'])
            tweet_txt.append(tweet['text'])
            tweet_loc.append(tweet['user']['location'])
            tweet_lang.append(tweet['lang'])
            screen_name.append(tweet['user']['screen_name'])
            followers_count.append(tweet['user']['followers_count'])
            retweet_count.append(tweet['retweet_count'])
            retweeted.append(tweet['retweeted'])
            replied_id.append(tweet['in_reply_to_status_id'])
    tweets_df = pd.DataFrame({'user_id': user_id, 'user_name': user_name, 'tweet_dt': tweet_dt,
                              'tweet_txt': tweet_txt, 'tweet_loc': tweet_loc, 'tweet_lang': tweet_lang,
                              'screen_name': screen_name, 'followers_count': followers_count,
                              'retweet_count': retweet_count, 'retweeted': retweeted,
                              'replied_id': replied_id})
    tweets_df = tweets_df[['user_id', 'user_name', 'tweet_dt', 'tweet_txt', 'tweet_loc', 'tweet_lang',
                           'screen_name', 'followers_count', 'retweet_count', 'retweeted', 'replied_id']]

    #User created indicator if each tweet starts with "RT " indicating a retweet:
    tweets_df["retweet_ind"] = tweets_df["tweet_txt"].str[:3]=="RT "
    return tweets_df


#Extracting Tweets to Dataframes and then to CSV files:
all_callaway_tweets_df = df_of_relevant_tweet_attributes(all_callaway_tweets)
all_callaway_tweets_df.to_csv("all_callaway_tweets.csv")

all_titleist_tweets_df = df_of_relevant_tweet_attributes(all_titleist_tweets)
all_titleist_tweets_df.to_csv("all_titleist_tweets.csv")

all_taylormade_tweets_df = df_of_relevant_tweet_attributes(all_taylormade_tweets)
all_taylormade_tweets_df.to_csv("all_taylormade_tweets.csv")

all_cobra_tweets_df = df_of_relevant_tweet_attributes(all_cobra_tweets)
all_cobra_tweets_df.to_csv("all_cobra_tweets.csv")

all_ping_tweets_df = df_of_relevant_tweet_attributes(all_ping_tweets)
all_ping_tweets_df.to_csv("all_ping_tweets.csv")

all_underarmour_tweets_df = df_of_relevant_tweet_attributes(all_underarmour_tweets)
all_underarmour_tweets_df.to_csv("all_underarmour_tweets.csv")



### Option to normalizing all json data into individual columns... However, too much data. ###
### That's why we selected specific variables instead.  ###
#//all_underarmour_tweets_df = json_normalize(all_underarmour_tweets) 
#//all_underarmour_tweets_df.to_csv("all_underarmour_tweets.csv")
