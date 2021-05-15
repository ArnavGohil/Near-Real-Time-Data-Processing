import requests
import datetime
import json


def getCenters():
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15'
    }

    i = 110001
    j = 110096

    lis = list()
    url = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode={}&date={}"
    y = datetime.datetime.now()
    date = y.strftime("%d-%m-%Y")

    while i <= j:
        response = requests.get(url.format(i, date), headers=headers, data=payload)
        parseJSON(response.json(), date, lis, y)
        i += 1

    return lis


def parseJSON(js, date, lis, dat):
    centres = js['centers']
    for center in centres:
        sessions = center['sessions']
        for ses in sessions:
            if (ses['date'] == date):
                val = dict()
                val['day'] = dat.day
                val['month'] = dat.month
                val['year'] = dat.year
                val['center_id'] = center['center_id']
                val['name'] = center['name']
                val['pincode'] = center['pincode']
                val['district_name'] = center['district_name']
                val['vaccine'] = ses['vaccine']
                val['min_age_limit'] = ses['min_age_limit']
                try:
                    if (center['fee_type'] == "Paid"):
                        val['fee'] = center['vaccine_fees'][0]['fee']
                    else:
                        val['fee'] = 0
                except:
                    val['fee'] = 0
                lis.append(val)


def writeJSON(lis, file):
    with open(file, "w") as outfile:
        json.dump(lis, outfile)


def main(file_name, **kwargs):
    lis = getCenters
    print(lis)
    writeJSON(lis, file_name)