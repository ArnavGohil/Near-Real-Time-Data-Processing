import os
import tweepy
import configparser
from google.cloud import vision
import re
import datetime
from PIL import Image
import requests
import io


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
    try:
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


def getNum(str):
    if len(str) == 0: return
    s = ""
    for char in str:
        if char.isdigit():
            s += char
    if len(s) == 0: return
    return int(s)


def cropTop(url , val ):
    im = Image.open(requests.get(url, stream=True).raw)

    # Setting the points for cropped image
    left = 500
    top = 127
    right = 700
    bottom = 300

    # Cropped image of above dimension
    im1 = im.crop((left, top, right, bottom))


    img_byte_arr = io.BytesIO()
    im1.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()

    image = vision.Image(content=img_byte_arr)

    client = vision.ImageAnnotatorClient()
    response = client.text_detection(image=image)
    texts = response.text_annotations
    string = ""
    for text in texts:
        string = text.description
        break


    l = 1
    for word in string.split("\n"):
        i = getNum(word)
        if i == 24: continue
        if l == 1 : val["positive"] = i
        if l == 2 : val["tests"] = i
        if l == 4 : val["recovered"] = i
        if l == 5 : val["deaths"] = i
        l+=1


    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))


def cropMid(url , val):
    im = Image.open(requests.get(url, stream=True).raw)

    # Setting the points for cropped image
    left = 525
    top = 590
    right = 700
    bottom = 750

    # Cropped image of above dimension
    im1 = im.crop((left, top, right, bottom))


    img_byte_arr = io.BytesIO()
    im1.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()

    image = vision.Image(content=img_byte_arr)

    client = vision.ImageAnnotatorClient()
    response = client.text_detection(image=image)
    texts = response.text_annotations

    string = ""
    for text in texts:
        string = text.description
        break

    l = 1
    for word in string.split("\n"):
        i = getNum(word)
        if i == 24: continue
        if l == 1 : val["vaccinated"] = i
        if l == 2 : val["first_dose"] = i
        if l == 3 : val["second_dose"] = i
        if l >= 4 : break
        l+=1

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))




def main(data_dict, **kwargs):
    api = initialise()
    url = getTweet(api)
    
    str = parseTweet(url)
    getValues(data_dict, str)
    
    cropTop(url ,data_dict)
    cropMid(url ,data_dict)

    y = datetime.datetime.now()
    y -= datetime.timedelta(days=1)
    data_dict['day'] = y.day
    data_dict['month'] = y.month
    data_dict['year'] = y.year

    print(data_dict)

