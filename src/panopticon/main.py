#!/usr/bin/env python

from gevent import monkey

monkey.patch_all()

import argparse
import csv
import json
import logging
import os
import random
import requests
import sys
import threading
import time

from PIL import Image
from io import BytesIO
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from flask import Flask, Response, redirect
from gevent.pywsgi import WSGIServer
from pathlib import Path

# Dictionary of info for all cameras
CAMERA_INFO = {}

IMAGE_CV = threading.Condition()
CAM_ID = 0
IMAGE_ID = 0
IMAGE = None

CAMERAS = {}
CAMERA_STATS = {}


def load_cameras(csv_file):
    cam_dict = {}
    with open(csv_file, "r") as f:
        cams = csv.DictReader(f)
        for cam in cams:
            cam_id = int(cam["Camera ID"])
            assert cam_id not in cam_dict
            assert cam["Screenshot Address"] != ""
            cam_dict[cam_id] = cam
    return cam_dict


def request_image(cam_id, handler=None):
    try:
        url = CAMERA_INFO[cam_id]['Screenshot Address']
        with requests.get(url, stream=True) as r:
            if r.status_code != 200:
                logging.info(f"got status {r.status_code} fetching {url}")
                return None
            if r.headers["Content-Type"] != "image/jpeg":
                logging.warning(f"got non-jpeg when fetching {url}")
                return None
            if r.headers["ETag"] == "3098b5594c26b8f0fd53420ad094f2df":
                logging.info(f"ETag indicates camera is offline {url}")
                return None

            metadata = {}
            metadata['cam_id'] = cam_id
            metadata["url"] = url
            metadata["ETag"] = r.headers["ETag"].strip('"')
            for key in ["Date", "Last-Modified", "Expires"]:
                metadata[key] = parsedate_to_datetime(r.headers[key])

            if handler is not None:
                handler(r.content, metadata)

            return metadata
    except Exception as e:
        logging.error(f"tried to fetch {url}, got {e}")
        return None


def get_delta(camera):
    return (camera["Expires"] - datetime.now(timezone.utc)).total_seconds()


def handle_new_image(image, metadata):
    global IMAGE_ID
    global CAM_ID
    global IMAGE_CV
    global IMAGE
    with IMAGE_CV:
        cam_id = metadata['cam_id']
        logging.info(f"setting new image to {cam_id}")
        IMAGE = image
        CAM_ID = cam_id
        IMAGE_ID += 1
        IMAGE_CV.notify_all()


def monitor_cameras(resolution):
    global CAMERAS
    for cam_id, cam in CAMERA_INFO.items():
        height = 0
        def get_height(content, metadata):
            nonlocal height
            with Image.open(BytesIO(content)) as image:
                height = image.height

        metadata = request_image(cam_id, get_height)
        if metadata is None:
            continue
        if height == resolution:
            CAMERAS[cam_id] = metadata

    global CAMERA_STATS
    for cam_id in CAMERAS.keys():
        CAMERA_STATS[cam_id] = {
            'hits': 0,
        }

    while True:
        expired = []
        expiring = []
        for cam_id, cam in CAMERAS.items():
            delta = get_delta(cam)
            if delta < 0:
                expired.append(cam_id)
            elif delta < 5:
                expiring.append(cam_id)

        logging.debug(f"expiring: {expiring}")
        logging.debug(f"expired: {expired}")

        found_one = False
        while len(expiring) > 0 and not found_one:
            logging.debug("Looping...")
            time.sleep(1)
            random.shuffle(expiring)
            for cam_id in expiring:
                cam = CAMERAS[cam_id]
                metadata = request_image(cam_id)
                if metadata is not None and (metadata["ETag"] != cam["ETag"]):
                    seconds = get_delta(cam)
                    logging.info(
                        f"{cam['url']} etag changed! {seconds} after expiration"
                    )
                    metadata = request_image(cam_id, handle_new_image)
                    if metadata is not None:
                        CAMERAS[cam_id] = metadata
                        CAMERA_STATS[cam_id]['hits'] += 1
                        found_one = True
                        break

        for cam_id in expired:
            metadata = request_image(cam_id)
            if metadata is not None:
                CAMERAS[cam_id] = metadata

        time.sleep(3)


app = Flask(__name__)

@app.route("/")
def index():
    return """
        <html>
        <head><title>panopticon</title></head>
        <body>
          <img src="/mjpeg" /><br/>
          <a href="/cur/loc">show camera location</a><br/>
          <a href="/cur/img">view just this camera</a><br/>
        </body>
        </html>
    """


@app.route("/mjpeg")
def mjpeg():
    def get_img():
        while True:
            with IMAGE_CV:
                yield b"\r\n".join(
                    [b"--frame", b"Content-Type: image/jpeg", b"", IMAGE, b""]
                )
                cur_id = IMAGE_ID
                while cur_id == IMAGE_ID:
                    IMAGE_CV.wait()
                logging.info(f"yielding new image! {cur_id}, {IMAGE_ID}")
    return Response(get_img(), mimetype="multipart/x-mixed-replace;boundary=frame")


@app.route("/stats")
def stats():
    return json.dumps(CAMERA_STATS)

@app.route("/info")
def info():
    return json.dumps(CAMERAS, default=str)


@app.route("/cur/img")
def cur_img():
    url = CAMERA_INFO[CAM_ID]['Screenshot Address']
    return redirect(url, 302)


@app.route("/cur/loc")
def cur_loc():
    loc = CAMERA_INFO[CAM_ID]['Location']
    _, lon, lat = loc.split(' ')
    lon = lon.strip('(')
    lat = lat.strip(')')

    # https://www.openstreetmap.org/?mlat=30.2522736&mlon=-97.7486496#map=18/30.2522736/-97.7486496
    zoom = 18
    url = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map={zoom}/{lat}/{lon}";
    return redirect(url, 302)


def main():
    parser = argparse.ArgumentParser(description="panopticon")

    parser.add_argument("--log", default="warning", action="store", help="log level")
    parser.add_argument(
        "--resolution",
        default=1080,
        action="store",
        type=int,
        help="resolution of cameras to monitor",
    )
    parser.add_argument(
        "--cameras",
        action="store",
        default=os.path.join(os.path.dirname(__file__), "Traffic_Cameras.csv"),
        help="path to CoA's traffic cameras CSV",
    )
    parser.add_argument(
        "--port", action="store", default=1292, type=int, help="port to serve on"
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.log.upper())

    global CAMERA_INFO
    CAMERA_INFO = load_cameras(args.cameras)

    global IMAGE
    with open(os.path.join(os.path.dirname(__file__), "init.jpg"), "rb") as f:
        IMAGE = f.read()

    mon_thread = threading.Thread(
        target=monitor_cameras, args=(args.resolution, )
    )
    mon_thread.start()

    server = WSGIServer(("", args.port), app)
    server.serve_forever()


if __name__ == "__main__":
    main()
