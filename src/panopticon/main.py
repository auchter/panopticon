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
from flask import Flask, Response
from gevent.pywsgi import WSGIServer
from pathlib import Path

IMAGE_CV = threading.Condition()
IMAGE_ID = 0
IMAGE = None


def load_cameras(csv_file):
    cam_dict = {}
    with open(csv_file, "r") as f:
        cams = csv.DictReader(f)
        for cam in cams:
            cam_id = int(cam["Camera ID"])
            assert cam_id not in cam_dict
            assert cam["Screenshot Address"] != ""
            cam_dict[cam_id] = cam
    return cam_dict.values()


def request_image(url, handler=None):
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
        metadata["url"] = url
        metadata["ETag"] = r.headers["ETag"].strip('"')
        for key in ["Date", "Last-Modified", "Expires"]:
            metadata[key] = parsedate_to_datetime(r.headers[key])

        if handler is not None:
            handler(r.content)

        return metadata


def get_delta(camera):
    return (camera["Expires"] - datetime.now(timezone.utc)).total_seconds()


def handle_new_image(image):
    global IMAGE_ID
    global IMAGE_CV
    global IMAGE
    with IMAGE_CV:
        IMAGE = image
        IMAGE_ID += 1
        IMAGE_CV.notify_all()


def monitor_cameras(csv, resolution):
    all_cameras = load_cameras(csv)
    cameras = {}
    for cam in all_cameras:
        url = cam["Screenshot Address"]
        cam_id = int(cam["Camera ID"])

        height = 0

        def get_height(content):
            nonlocal height
            with Image.open(BytesIO(content)) as image:
                height = image.height

        metadata = request_image(url, get_height)
        if metadata is None:
            continue
        if height == resolution:
            cameras[cam_id] = metadata

    while True:
        expired = []
        expiring = []
        for cam_id, cam in cameras.items():
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
                cam = cameras[cam_id]
                metadata = request_image(cam["url"])
                if metadata["ETag"] != cam["ETag"]:
                    seconds = get_delta(cam)
                    logging.info(
                        f"{cam['url']} etag changed! {seconds} after expiration"
                    )
                    cameras[cam_id] = request_image(cam["url"], handle_new_image)
                    found_one = True

        for cam_id in expired:
            cam = cameras[cam_id]
            cameras[cam_id] = request_image(cam["url"])

        time.sleep(3)


def get_img():
    global IMAGE_ID
    global IMAGE_CV
    global IMAGE
    while True:
        with IMAGE_CV:
            yield b"\r\n".join(
                [b"--frame", b"Content-Type: image/jpeg", b"", IMAGE, b""]
            )
            cur_id = IMAGE_ID
            while cur_id == IMAGE_ID:
                IMAGE_CV.wait()
            logging.info(f"yielding new image! {cur_id}, {IMAGE_ID}")


app = Flask(__name__)


@app.route("/")
def index():
    return """
        <html>
        <head><title>panopticon</title></head>
        <body><img src="/mjpeg" /></body>
        </html>
    """


@app.route("/mjpeg")
def mjpeg():
    return Response(get_img(), mimetype="multipart/x-mixed-replace;boundary=frame")


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

    global IMAGE
    with open(os.path.join(os.path.dirname(__file__), "init.jpg"), "rb") as f:
        IMAGE = f.read()

    mon_thread = threading.Thread(
        target=monitor_cameras, args=(args.cameras, args.resolution)
    )
    mon_thread.start()

    server = WSGIServer(("", args.port), app)
    server.serve_forever()


if __name__ == "__main__":
    main()
