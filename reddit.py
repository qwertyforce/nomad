import datetime as dt
from PIL import Image
from psaw import PushshiftAPI
api = PushshiftAPI()
from requests import get,head  # to make GET request
import os
import re
import io 

EXTENSION={"image/jpeg":".jpg","image/png":".png"}
ALLOWED_MIME=["image/jpeg", "image/png"]
imgur_client_id=""

def get_mime(url):
    try:
        x = head(url,timeout=5)
        ext1=("content-type" in x.headers)
        if ext1:
            return x.headers["content-type"]
        ext2=("Content-Type" in x.headers)
        if ext2:
            return x.headers["Content-Type"]
        return False
    except:
        return False


def download(url, file_name,ext):
    try:
        full_file_name=file_name+ext
        if os.path.isfile(full_file_name):
            print("File exist")
            return
        print(f"downloading {url} as {full_file_name}")
        
        response = get(url,timeout=5)
        fake_file = io.BytesIO(response.content)
        im = Image.open(fake_file)
        width, height = im.size
        if width*height < 1280 * 1024:
            return
        with open(full_file_name, "wb") as file:
            file.write(response.content)
    except:
        print("error "+ url)

def handle_imgur(imgur_link):
    try:
        album_id=re.search('(?<=\/a\/)(.*)', imgur_link).group(0)
        if album_id:
            print(f"downloading album {imgur_link}")
            url = f"https://api.imgur.com/3/album/{album_id}/images"
            headers = {'Authorization': f'Client-ID {imgur_client_id}'}
            response = get(url, headers=headers,timeout=5)
            data = response.json()
            # print(data)
            for img in data["data"]:
                if img["type"] in ALLOWED_MIME:
                    download(img["link"],img["id"],EXTENSION[img["type"]])
    except:
        print("error "+ imgur_link)

def scrape_reddit():
    # before_epoch = int(dt.datetime.now().timestamp())
    before_epoch=int(dt.datetime(2019, 10, 31).timestamp())
    start_epoch = before_epoch-1209600
    empt=0
    while True:
        print('===iteration===')
        print(dt.datetime.utcfromtimestamp(start_epoch).strftime('%d-%m-%Y'))
        print(dt.datetime.utcfromtimestamp(before_epoch).strftime('%d-%m-%Y'))
        subs = list(api.search_submissions(after=start_epoch,before=before_epoch, subreddit='Earthporn', sort_type='score',sort='desc', filter=['id', 'url', 'title','score', 'permalink'],limit=100))
        if len(subs) == 0:
            empt+=1
        else:
            empt=0
        if empt==10:
            break
        before_epoch -= 1209600
        start_epoch -= 1209600
        for x in subs:
            file_mime=get_mime(x.url)
            if file_mime in ALLOWED_MIME:
                file_ext=EXTENSION[file_mime]
                download(x.url, x.id,file_ext)
            elif "imgur.com/a" in x.url:
                handle_imgur(x.url)
scrape_reddit()