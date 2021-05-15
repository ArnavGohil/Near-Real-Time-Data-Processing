import os
import tweepy
import configparser
from google.cloud import vision
import re
import datetime


def initialise():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    consumer_key = config.get('KEY', 'consumer_key')
    consumer_secret = config.get('KEY', 'consumer_secret')
    access_token = config.get('KEY', 'access_token')
    access_token_secret = config.get('KEY', 'access_token_secret')

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config.get('KEY','json_loc')

    return api


def getTweet(api):
    search_words = "Delhi Health Bulletin (from:CMODelhi)"

    tweets = tweepy.Cursor(api.search,
                           q=search_words,
                           lang="en", ).items(1)

    image_url = ""
    # Iterate and print tweets
    for tweet in tweets:
        media = tweet.entities.get('media', [])
        image_url = media[0]['media_url']

    return image_url


def parseTweet(image_url):
    client = vision.ImageAnnotatorClient()
    image = vision.Image()
    image.source.image_uri = image_url

    response = client.text_detection(image=image)

    string = ""
    for text in response.text_annotations:
        string = text.description
        break

    return string


def getValues(val, string):
    y = datetime.datetime.now()
    y -= datetime.timedelta(days=1)
    try:
        # Date
        match = (re.search("/ ", string))
        i = int(match.end())
        match = (re.search(")", string))
        j = int(match.start())
        print(string[i:j])
        val['day'] = y.day
        val['month'] = y.month
        val['year'] = y.year

        # +ve cases
        i = string.find("Positive Cases", string.find("Positive Cases") + 1)
        match = (re.search("Tests Conducted", string))
        j = int(match.start())
        val['positive'] = string[i + len("Positive Cases") + 1: j - 1]

        # Tests
        match = (re.search("Tests Conducted", string))
        i = int(match.end())
        match = (re.search("Positivity Rate", string))
        j = int(match.start())
        val['tests'] = string[i + 1:j - 1]

        # Recovered
        match = (re.search("%", string))
        i = int(match.end())
        match = (re.search("Deaths", string))
        j = int(match.start())
        val['recovered'] = string[i + 1:j - 1]

        # Deaths
        match = (re.search("Deaths", string))
        i = int(match.end())
        match = (re.search("COVID-19 Patient Management", string))
        j = int(match.start())
        val['deaths'] = string[i + 1:j - 1]

        # Tot Vac
        match = (re.search("Beneficiaries vaccinated in last 24 hours", string))
        i = int(match.end())
        match = (re.search("Beneficiaries vaccinated 1", string))
        j = int(match.start())
        val['vaccinated'] = string[i + 1:j - 1]

        # 1st dose
        match = (re.search("dose in last 24 hours", string))
        i = int(match.end())
        match = (re.search("Beneficiaries vaccinated 2", string))
        j = int(match.start())
        val['first_dose'] = string[i + 1:j - 1]

        # 2nd dose
        pat = "dose in last 24 hours"
        pos = string.find(pat) + 1
        p1 = string.find(pat, pos + 1)
        i = p1 + len(pat)
        match = (re.search("Cumulative beneficiaries vaccinated so far", string))
        j = int(match.start())
        val['second_dose'] = string[i + 1:j - 1]

        # Active Cases
        match = (re.search("Active Cases", string))
        i = int(match.end())
        match = (re.search("Total Number of Containment Zones as on date", string))
        j = int(match.start())
        val['active_cases'] = string[i + 1:j - 1]

        # Containment
        match = (re.search("Total Number of Containment Zones as on date: ", string))
        i = int(match.end())
        match = (re.search("Calls received", string))
        j = int(match.start())
        val['zones'] = string[i:j - 1]

    except:
        print("Error caught")
        return None


def main(data_dict, **kwargs):
    print("Aur Bhai")
    api = initialise()
    url = getTweet(api)
    str = parseTweet(url)
    getValues(data_dict, str)