import time
from datetime import timezone
import uuid
import json
import datetime
import RPi.GPIO as GPIO
from gpiozero import MotionSensor
import picamera
import paho.mqtt.client as mqtt
import os

from minio import Minio
from minio.error import S3Error
from subprocess import call


PIR_PIN = 26
pir = MotionSensor(PIR_PIN)
VIDEO_TIME_SEC = 15
BROKER_ADDRESS = "185.251.89.148"

MINIO_HOST = "185.251.89.148:443"
BUCKET_NAME = 'raspberrycamera'

FILE_DIR = 'snapshots/'
MP4_VIDEO_EXT = '.mp4'
H264_VIDEO_EXT = '.h264'
camera = picamera.PiCamera()
camera.resolution = 640,480
GPIO.setup(PIR_PIN, GPIO.IN)

mqttClient = mqtt.Client("P1")
mqttClient.loop_start()
mqttClient.connect(BROKER_ADDRESS)

client = Minio(
        MINIO_HOST,
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
found = client.bucket_exists(BUCKET_NAME)
if not found:
    client.make_bucket(BUCKET_NAME)
else:
    print("Bucket {} already exists".format(BUCKET_NAME))
    
def getDeviceId():
    try:
        deviceUUIDFile  = open("device_uuid", "r")
        deviceUUID = deviceUUIDFile.read()
        print("Device UUID : " + deviceUUID)
        return deviceUUID
    except FileNotFoundError:
        print("Configuring new UUID for this device...")
        deviceUUIDFile = open("device_uuid", "w")
        deviceUUID = str(uuid.uuid4())
        print("Device UUID : " + deviceUUID)
        deviceUUIDFile.write(deviceUUID)
        return deviceUUID
        


def record(filename):
    h264_file = filename + H264_VIDEO_EXT
    print("Recording : " + h264_file)
    camera.start_recording(h264_file)
    camera.wait_recording(VIDEO_TIME_SEC)
    camera.stop_recording()
    print("Recorded")
    
    mp4_file = filename + MP4_VIDEO_EXT
    command = "MP4Box -add " + h264_file + " " + mp4_file
    print("Converting from .h264 to mp4")
    
    call([command], shell=True)
    print("Converted")


def sendToMinio(filename):
    try:
        print("Sending to minio")
        client.fput_object(
            BUCKET_NAME, filename, FILE_DIR + filename
        )
        print("Video has been sent")
    except Exception as e:
        print(e)
        print("Couldn't send to Minio")
        
def removeFiles(filename):
    if os.path.exists(FILE_DIR + filename + H264_VIDEO_EXT):
        os.remove(FILE_DIR + filename + H264_VIDEO_EXT)
    if os.path.exists(FILE_DIR + filename + MP4_VIDEO_EXT):
        os.remove(FILE_DIR + filename + MP4_VIDEO_EXT)


time.sleep(5)
deviceUUID = getDeviceId()

try:
    while True:
        pir.wait_for_motion()
        dt = datetime.datetime.utcnow()
        st = dt.strftime('%d.%m.%Y %H:%M:%S')
        print("Motion Detected at : " + st)
        alarmUUID = str(uuid.uuid4())
        filename = '{}_alarm'.format(alarmUUID)
        message = json.dumps({
                                'device_id': deviceUUID,
                                'id': alarmUUID,
                                'place': 'office_room',
                                'filename': filename + MP4_VIDEO_EXT,
                                'type': 'detected_motion',
                                'occurred_at': st
                                }, sort_keys=True)
        mqttClient.publish("raspberry/main", message)
        record(FILE_DIR + filename)
        sendToMinio(filename + MP4_VIDEO_EXT)
        removeFiles(filename)
        time.sleep(8)
        
except Exception as e:
    print(e)
    print("Quit")
    mqttClient.loop_stop()
    GPIO.cleanup()
    
