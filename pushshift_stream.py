import datetime as dt
from PIL import Image
from requests import get, post, head
import os
import re
import io 
import json
from tqdm import tqdm
from time import sleep
import concurrent.futures
MAX_WORKERS = 32

IMG_PATH = "./test/"
JSON_PATH  = "./test_json/"

try:
    os.mkdir(IMG_PATH)
    os.mkdir(JSON_PATH)
except:
    print("yeah")

import numpy as  np

POST_TO_SCENERY = False

import flickrapi
api_key = u'api_key'
api_secret = u'api_secret'

flickr = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')

EXTENSION={"image/jpg":".jpg","image/jpeg":".jpg","image/png":".png"}
ALLOWED_MIME=["image/jpg","image/jpeg", "image/png"]
imgur_client_id="imgur_client_id"

import zmq
context = zmq.Context()
print("Connecting to anti_sus server…")
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:7777")

def b58decode(s):
    alphabet = '123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ'
    num = len(s)
    decoded = 0 
    multi = 1
    for i in reversed(range(0, num)):
        decoded = decoded + multi * ( alphabet.index( s[i] ) )
        multi = multi * len(alphabet)
    return decoded
    
def get_mime(url):
    try:
        x = head(url,timeout=5)
        if "content-type" in x.headers:
            return x.headers["content-type"]
        if "Content-Type" in x.headers:
            return x.headers["Content-Type"]
        return False
    except:
        return False

def check_fit(images):
    socket.send(images.tobytes())
    message = socket.recv()
    return list(np.frombuffer(message,dtype=np.int32))

def download(url, file_name, ext, post_idx):
    try:
        if ext is None:
            file_mime=get_mime(url)
            if file_mime in ALLOWED_MIME:
                ext=EXTENSION[file_mime]
            else:
                return
            
        full_file_name=IMG_PATH+file_name+ext
        if os.path.isfile(full_file_name):
            # print("File exist")
            return False
        
        response = get(url,timeout=5)
        fake_file = io.BytesIO(response.content)
        im = Image.open(fake_file)
        width, height = im.size
        if width*height < 1280 * 1024:
            # print("size too small")
            return False
        if im.mode != 'RGB':
            im = im.convert('RGB')
        return (post_idx, np.array(im.resize((448,448),Image.Resampling.LANCZOS)), full_file_name, response.content)
    except Exception as e:
        print(e)
        print("error "+ url)

def handle_imgur(post, imgur_link):
    try:
        album_id=re.search('(?<=\/a\/)(.*)', imgur_link).group(0)
        if album_id:
            print(f"downloading album {imgur_link}")
            url = f"https://api.imgur.com/3/album/{album_id}/images"
            headers = {'Authorization': f'Client-ID {imgur_client_id}'}
            response = get(url, headers=headers,timeout=5)
            data = response.json()
            post__img_urls = []
            for img in data["data"]:
                if img["type"] in ALLOWED_MIME:
                   post__img_urls.append((post, img["link"], post["id"]+"_imgur_"+img["id"], EXTENSION[img["type"]]))

            return post__img_urls
    except Exception as e:
        print(e)
        print("error "+ imgur_link)
        return []


def get_post__img_urls(posts):
    post__img_urls=[]
    for post in tqdm(posts):
        if post["over_18"] or post["is_video"] or post["removed_by_category"]:
            continue
        if ("media_metadata" in post and post["media_metadata"]) or ("crosspost_parent_list" in post and post["crosspost_parent_list"] and "media_metadata" in post["crosspost_parent_list"][0]):
            if "media_metadata" in post and post["media_metadata"]:
                media_metadata_obj = post["media_metadata"].values()
            elif post["crosspost_parent_list"][0]['media_metadata']:
                media_metadata_obj = post["crosspost_parent_list"][0]['media_metadata'].values()
            for obj in media_metadata_obj:
                if "e" in obj and obj["e"] == "Image":
                    file_mime = obj["m"]
                    img_id = obj['id']
                    if file_mime in ALLOWED_MIME:
                        file_ext=EXTENSION[file_mime]
                        img_url = f"https://i.redd.it/{img_id}{file_ext}"
                        post__img_urls.append((post, img_url, f"{post['id']}_reddit_{img_id}", file_ext))
        else:

            if "imgur.com/a/" in post["url"] or "imgur.com/gallery" in post["url"]:
                post__img_urls.extend(handle_imgur(post,post["url"].replace("gallery","a")))

            elif "://imgur.com/" in post["url"] and not "." in post["url"]:
                img_url = post["url"] + ".jpg"
                post__img_urls.append((post,img_url, post["id"], None))
                
            elif "flic.kr/p/" in post["url"]:
                try:
                    start_id = post["url"].find("flic.kr/p/") + 10
                    end_id = post["url"].find("/",start_id)
                    if end_id == -1:
                        id = b58decode(post["url"][start_id:])
                    else:
                        id = b58decode(post["url"][start_id:end_id])
                    id = str(id)
                    sizes = flickr.photos_getSizes(photo_id=id)
                    img = sizes["sizes"]["size"][-1]
                    if img["media"] == "photo":
                        img_url = img["source"]
                        post__img_urls.append((post,img_url, post["id"], None))
                except:
                    print("flic.kr/p/", post["url"])

            elif "flickr.com/photos/" in post["url"]:
                try:
                    start_id = post["url"].find("/",post["url"].index("/photos/")+8)+1
                    end_id = post["url"].find("/",start_id)
                    if end_id == -1:
                        id = post["url"][start_id:]
                    else:
                        id = post["url"][start_id:end_id]

                    sizes = flickr.photos_getSizes(photo_id=id)
                    img = sizes["sizes"]["size"][-1]
                    if img["media"] == "photo":
                        img_url = img["source"]
                        post__img_urls.append((post,img_url, post["id"],None))
                except Exception as e:
                    print("flickr error ",post["url"])

            elif "staticflickr.com" in post["url"]:
                try:
                    start_id = post["url"].find(".jpg")
                    start_id = post["url"].rfind("_",0, post["url"].rfind("_",0,start_id))
                    end_id = post["url"].rfind("/",0,start_id)+1
                    id = post["url"][end_id:start_id]
                    if id.find("_") != -1:
                        id = id[:id.find("_")]
                    sizes = flickr.photos_getSizes(photo_id=id)
                    img = sizes["sizes"]["size"][-1]
                    # print(img)
                    if img["media"] == "photo":
                        img_url = img["source"]
                        post__img_urls.append((post, img_url, post["id"], None))
                except Exception as e: 
                    print(e)
                    print("staticflickr error ",post["url"])
                    print("trying get flickr image directly",post["url"])
                    post__img_urls.append((post, post["url"], post["id"], None))
            else:
                post__img_urls.append((post, post["url"], post["id"], None))
    return post__img_urls


urls_broken = []
def scrape_reddit():
    after_epoch =0
    empty_results=0
    itertation_num = 0
    while True:
        itertation_num+=1
        print('===iteration===')
        print(dt.datetime.now())
        data = get(f"https://api.pushshift.io/reddit/submission/search?sort=created_utc&order=desc&filter=id,author,url,title,score,permalink,over_18,is_video,removed_by_category,crosspost_parent_list,media_metadata,gallery_data,selftext,created_utc&limit=1000&after={after_epoch}")
        posts = data.json()["data"]
        
        if len(posts) == 0:
            print(f"after = {after_epoch}")
            sleep(20)
            empty_results+=1
        else:
            after_epoch = int(dt.datetime.now().timestamp())
            empty_results=0

        if empty_results==50:
            break

        post__img_urls = get_post__img_urls(posts)
        post_idx__img = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = (executor.submit(download, obj[1],obj[2],obj[3],post_idx) for post_idx, obj in enumerate(post__img_urls))
            for future in tqdm(concurrent.futures.as_completed(future_to_url)):
                try:
                    data = future.result()
                except Exception as exc:
                    print(exc)
                finally:
                    if data:
                        post_idx__img.append(data)            
        
        batch_size = 256
        for start_pos in tqdm(range(0,len(post_idx__img),batch_size)):
            batch = post_idx__img[start_pos:start_pos + batch_size]
            img_batch= np.array([x[1] for x in batch])
            print(img_batch.shape)
            print(img_batch.dtype)
            check_fit_res = check_fit(img_batch)
            print(check_fit_res)
            for in_batch_idx in check_fit_res:
                post_idx = batch[in_batch_idx][0]
                post_data = post__img_urls[post_idx]
                source_url = "https://reddit.com" + post_data[0]["permalink"]
                with open(batch[in_batch_idx][2], "wb") as file:
                    if POST_TO_SCENERY:
                        post('http://127.0.0.1/import_image', files=dict(image=batch[in_batch_idx][3]), data=dict(source_url=source_url,tags='["from_nomad"]',import_images_bot_password="123"))
                    file.write(batch[in_batch_idx][3])

            uniq_post_idxs = set([batch[in_batch_idx][0] for in_batch_idx in check_fit_res])
            for post_idx in uniq_post_idxs:
                post_data = post__img_urls[post_idx]
                with open(JSON_PATH+post_data[0]["id"]+".json", "w") as file:
                    file.write(json.dumps(post_data[0]))
scrape_reddit()


