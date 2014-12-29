# twitter function to feth reply
# copyright 2014 Tony P Francis
# See LICENSE for details


import os
import json
import sys
import time
import math

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

reload(sys)
sys.setdefaultencoding("UTF-8")



query = ''       # query string
query_limit = 200               # tweet query limit

path = os.path.dirname(os.path.abspath(__file__))       # path of this file

# removing below files if exist
if 'out_tweets.json' in os.listdir(path):
    os.remove('out_tweets.json')
if 'out_reply.json' in os.listdir(path):
    os.remove('out_reply.json')

out_file_tweets = open(os.path.join(path, 'out_tweets.json'), 'w+')     # file to store tweet related data
out_file_reply = open(os.path.join(path, 'out_reply.json'), 'w')        # file to store reply related data


'''
Specify twitter app credentials to access data
'''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_TOKEN_SECRET = ''


class TweetListener(StreamListener):
    '''
    Class object used to fetch and save the tweets
    '''

    tweet_list = {}         # dict to store tweets information
    retweet_list = {}           # dict to store re-tweets information
    all_tweet_list = {}         # dict to store above two dict

    def __init__(self, file):
        '''
        Constructor of TweetListener.

        Accepts a file pointer to store 'all_tweet_list'
        '''

        self.file = file        # file to store 'all_tweet_list'
        self.limit = 0          # limit to stop tweet stream wrt 'query_limit'

    def on_data(self, data):
        '''
        Tweepy function to get tweet
        '''

        if self.limit < query_limit:        # fetches the tweet till limit is reached
        #if raw_input("Continue (y/n)?") == 'y':        # fetches the tweet depending on users y/n
            print "*** TWEET *** = "+str(self.limit+1)
            TweetListener.all_tweet_list = get_tweets(json.loads(data))
            self.limit += 1
            return True
        else:
            print "*** Saving tweets ***"
            json.dump(TweetListener.all_tweet_list, self.file)      # saves all_tweet_list to give file
            return False        # stops the stream

    def on_error(self, status):
        '''
        Tweepy function to handle error
        '''

        string = ''
        if status == 400:
            string = "Bad Request"
        elif status == 401:
            string = "Unauthorized"
        elif status == 406:
            string = "Not Acceptable"
        elif status == 420:
            string = "Enhance Your Calm"
        elif status == 429:
            string = "Too Many Requests"
        elif status == 500:
            string = "Internal Server Error"
        elif status == 503:
            string = "Service Unavailable"
        print "ERROR %s : %s" % (status, string)
        return True

    def on_timeout(self):
        '''
        Tweepy fuction to handle timeout
        '''

        print "Timeout, sleeping for 10 seconds...\n"
        time.sleep(10)
        return True


class ReplyListener(StreamListener):
    '''
    Tweepy class to handle reply tweets
    '''

    tweet_id_list = []          # list to store tweet id
    tweet_user_list = []        # list to store user id
    tweet_reply = {}            # dict to store reply of tweets
    tweet_id_to_reply_id = {}   # dict to store tweet id and its reply tweet ids
    reply_count = 0             # counter to count reply
    error_flag = False          # flag to control error case

    new_reply_id = False        # flag to handle new reply_id
    new_reply_user = False      # flag to handle new reply user

    def __init__(self, file):
        self.file = file

    def on_data(self, data):
        self.tweet_reply, self.new_reply_id, self.new_reply_user = get_replies(json.loads(data))
        print "new_reply_id %s" % self.new_reply_id
        print "new_reply_user %s" % self.new_reply_user
        print "\n------------------------"
        print "Current tweet reply dict"
        print "------------------------"
        print self.tweet_reply
        print "************************\n"
        self.file.seek(0, 0)
        json.dump(self.tweet_reply, self.file)
        ReplyListener.error_flag = False
        if self.new_reply_user:
            return False
        else:
            return True

    def on_error(self, status):
        ReplyListener.error_flag = True
        string = ''
        if status == 400:
            string = "Bad Request"
        elif status == 401:
            string = "Unauthorized"
        elif status == 406:
            string = "Not Acceptable"
        elif status == 420:
            string = "Enhance Your Calm"
        elif status == 429:
            string = "Too Many Requests"
        elif status == 500:
            string = "Internal Server Error"
        elif status == 503:
            string = "Service Unavailable"
        print "ERROR %s : %s" % (status, string)
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 10 seconds...\n")
        time.sleep(10)
        return True

def tweet_classify(tweet):
    '''
    function to classify as tweet or retweet
    '''

    try:
        # returns true if its retweet
        if tweet['retweeted_status']:
            return True
    except KeyError:
        # returns false if its tweet
        return False

def get_tweets(tweet):
    '''
    processing individual tweets of query string
    '''

    print tweet['id_str']+" : "+tweet['text']+"***"+tweet['user']['screen_name']

    # seperating data wrt tweets and retweets
    if tweet_classify(tweet):
        # handles retweets

        if str(tweet['retweeted_status']['id_str']) not in ReplyListener.tweet_id_list:
            ReplyListener.tweet_id_list.append(str(tweet['retweeted_status']['id_str']))
        if '@'+str(tweet['user']['screen_name']) not in ReplyListener.tweet_user_list:
            print "***New User"
            print '@'+str(tweet['user']['screen_name'])
            ReplyListener.tweet_user_list.append('@'+str(tweet['user']['screen_name']))
        if str(tweet['retweeted_status']['id_str']) not in ReplyListener.tweet_id_to_reply_id.keys():
            ReplyListener.tweet_id_to_reply_id[str(tweet['retweeted_status']['id_str'])] = []
        TweetListener.retweet_list[tweet['id_str']] =\
            dict([
                    ('id_str', tweet['id_str']),
                    ('text', tweet['text']),
                    ('user', dict([
                                    ('user_id_str', tweet['user']['id_str']),
                                    ('user_screen_name', tweet['user']['screen_name']),
                                ])
                    ),
                    ('retweet_count', tweet['retweet_count']),
                    ('favorite_count', tweet['favorite_count']),
                    ('retweeted_status', dict([
                                                ('retweet_id_str', tweet['retweeted_status']['id_str']),
                                                ('retweet_text', tweet['retweeted_status']['text']),
                                                ('retweet_user', dict([
                                                                    ('retweet_user_id_str', tweet['retweeted_status']['user']['id_str']),
                                                                    ('retweet_user_screen_name', tweet['retweeted_status']['user']['screen_name']),
                                                                ])
                                                ),
                                            ])
                    ),
                    ('entities', dict([
                                        ('user_mentions', dict([
                                                                ('id_str', [x['id_str'] for x in tweet['entities']['user_mentions']]),
                                                                ('screen_name', [x['screen_name'] for x in tweet['entities']['user_mentions']]),
                                                            ])
                                        )
                                    ])
                    ),
                ])
    else:
        if str(tweet['id_str']) not in ReplyListener.tweet_id_list:
            ReplyListener.tweet_id_list.append(str(tweet['id_str']))
        if '@'+str(tweet['user']['screen_name']) not in ReplyListener.tweet_user_list:
            ReplyListener.tweet_user_list.append('@'+str(tweet['user']['screen_name']))
            print "***New User"
            print '@'+str(tweet['user']['screen_name'])
        if str(tweet['id_str']) not in ReplyListener.tweet_id_to_reply_id.keys():
            ReplyListener.tweet_id_to_reply_id[str(tweet['id_str'])] = []
        TweetListener.tweet_list[tweet['id_str']] =\
            dict([
                    ('id_str', tweet['id_str']),
                    ('text', tweet['text']),
                    ('user', dict([
                                    ('user_id_str', tweet['user']['id_str']),
                                    ('user_screen_name', tweet['user']['screen_name']),
                                ])
                    ),
                    ('retweet_count', tweet['retweet_count']),
                    ('favorite_count', tweet['favorite_count']),
                ])

    TweetListener.all_tweet_list['tweet_list'] = TweetListener.tweet_list
    TweetListener.all_tweet_list['retweet_list'] = TweetListener.retweet_list
    return dict(TweetListener.all_tweet_list)

def get_replies(reply):
    '''
    function to process replies of a tweet
    '''
    new_reply_id = False
    new_reply_user = False
    print "\nREPLY\n------"
    print "in_reply_to_status_id_str = %s" % reply['in_reply_to_status_id_str']
    print "id_str = %s" % reply['id_str']
    print "text = \n %s" % reply['text']
    print "user = %s\n" % reply['user']['screen_name']

    if reply['in_reply_to_status_id_str'] in ReplyListener.tweet_id_list:
        print "REPLY FOR TWEET %s FOUND"%(str(reply['in_reply_to_status_id_str']))
        print reply['id_str']+" : "+reply['text']+"***"+reply['user']['screen_name']

        if str(reply['id_str']) not in ReplyListener.tweet_id_list:
            ReplyListener.tweet_id_list.append(str(reply['id_str']))
            new_reply_id = True
        print "New tweet_id_list %s" % ReplyListener.tweet_id_list
        if '@'+str(reply['user']['screen_name']) not in ReplyListener.tweet_user_list:
            print "***New User"
            print '@'+str(reply['user']['screen_name'])
            ReplyListener.tweet_user_list.append('@'+str(reply['user']['screen_name']))
            new_reply_user = True
        print "New tweet_user_list %s" % ReplyListener.tweet_user_list

        if reply['in_reply_to_status_id_str'] in ReplyListener.tweet_id_to_reply_id.keys():
            print "\n***Found direct reply for %s" % reply['in_reply_to_status_id_str']
            print "OLD"
            print ReplyListener.tweet_id_to_reply_id[str(reply['in_reply_to_status_id_str'])]
            ReplyListener.tweet_id_to_reply_id[str(reply['in_reply_to_status_id_str'])].append(str(reply['id_str']))
            print "NEW"
            print ReplyListener.tweet_id_to_reply_id[str(reply['in_reply_to_status_id_str'])]
            if str(reply['in_reply_to_status_id_str']) not in ReplyListener.tweet_reply.keys():
                print "Appending reply to NEWLY"
                ReplyListener.tweet_reply[str(reply['in_reply_to_status_id_str'])] = []
                ReplyListener.tweet_reply[str(reply['in_reply_to_status_id_str'])].append(
                                                                                        dict(
                                                                                                [
                                                                                                tuple([reply['id_str'], [reply['text'], reply['user']['screen_name']]])
                                                                                                ]
                                                                                            )
                                                                                        )
            else:
                print "Appending reply to EXISTING"
                ReplyListener.tweet_reply[str(reply['in_reply_to_status_id_str'])].append(
                                                                                        dict(
                                                                                                [
                                                                                                tuple([reply['id_str'], [reply['text'], reply['user']['screen_name']]])
                                                                                                ]
                                                                                            )
                                                                                        )
        else:
            for k, v in ReplyListener.tweet_id_to_reply_id.items():
                if str(reply['in_reply_to_status_id_str']) in v:
                    print "\n***Found reply to reply for %s" % reply['in_reply_to_status_id_str']
                    print "OLD"
                    print ReplyListener.tweet_id_to_reply_id[str(k)]
                    ReplyListener.tweet_id_to_reply_id[str(k)].append(str(reply['id_str']))
                    print "NEW"
                    print ReplyListener.tweet_id_to_reply_id[str(k)]
                    if str(k) not in ReplyListener.tweet_reply.keys():
                        ReplyListener.tweet_reply[str(k)] = []
                        ReplyListener.tweet_reply[str(k)].append(
                                                                dict(
                                                                        [
                                                                        tuple([reply['id_str'], [reply['text'], reply['user']['screen_name']]])
                                                                        ]
                                                                    )
                                                                )
                    else:
                        ReplyListener.tweet_reply[str(k)].append(
                                                                dict(
                                                                        [
                                                                        tuple([reply['id_str'], [reply['text'], reply['user']['screen_name']]])
                                                                        ]
                                                                    )
                                                                )


        print "ReplyListener.tweet_id_to_reply_id"
        print ReplyListener.tweet_id_to_reply_id
        ReplyListener.reply_count += 1

        print "Reply count is "+str(ReplyListener.reply_count)
    return ReplyListener.tweet_reply, new_reply_id, new_reply_user





'''
Begining of the twitter tweet fetching process
'''
# instantiates an object of TweetListener with output file
TListener = TweetListener(out_file_tweets)

# creates an auth object
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# creates streaming object
tStream = Stream(auth, TListener)
print "*** Streaming "+str(query_limit)+" tweets with tag "+query

# fetchs tweets with 'query'
tStream.filter(track=[query])

# closing file out_file_tweets
print "closing file out_file_tweets"
out_file_tweets.close()
print "ReplyListener.tweet_id_list"
print ReplyListener.tweet_id_list
print "ReplyListener.tweet_user_list"
print ReplyListener.tweet_user_list
print "length of ReplyListener.tweet_user_list = %d" % len(ReplyListener.tweet_user_list)
print "Sleepting for 3 sec"
time.sleep(3)

#FETCHING REPLIES FOR TWEETS
RListner = ReplyListener(out_file_reply)

print "\n*** FETCHING REPLIES FOR TWEET'S "
while True:
    rStream = Stream(auth, RListner)
    print "Rerunning stream with ReplyListener.tweet_user_list %s" % ReplyListener.tweet_user_list
    print "length of ReplyListener.tweet_user_list = %d" % len(ReplyListener.tweet_user_list)

    rStream.filter(track=ReplyListener.tweet_user_list[-199:])
    rStream.disconnect()
    if ReplyListener.error_flag:
        print "Sleeping for 120 sec"
        time.sleep(120)
    print "Rerunning stream with updated ReplyListener.tweet_user_list %s" % ReplyListener.tweet_user_list
    del rStream

