import time
import uuid
import json
import datetime
import RPi.GPIO as GPIO
import picamera
from minio import Minio
from minio.error import S3Error

import paho.mqtt.client as mqtt

GPIO.setmode(GPIO.BCM)

PIR_PIN = 26
VIDEO_TIME_SEC = 5
BROKER_ADDRESS = "192.168.1.9"

MINIO_HOST = "192.168.1.9:443"
BUCKET_NAME = 'raspberrycamera'

FILE_DIR = 'snapshots/'
camera = picamera.PiCamera()
camera.resolution = 640,480
GPIO.setup(PIR_PIN, GPIO.IN)

mqttClient = mqtt.Client("P1")
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
    print("Recording : " + filename)
    camera.start_recording(filename)
    camera.wait_recording(VIDEO_TIME_SEC)
    camera.stop_recording()
    print("Recorded")


def sendToMinio(filename):
    try:
        print("Sending to minio")
        client.fput_object(
            BUCKET_NAME, filename, FILE_DIR + filename
        )
        print("Video has been sent")
    except Exception:
        print("Couldn't send to Minio")


time.sleep(2)
deviceUUID = getDeviceId()
print('Ready. Device ID : {}'.format(deviceUUID))

try:
    while True:
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M:%S')
        if GPIO.input(PIR_PIN):
            time.sleep(0.5)
            print("Motion Detected... Sending to MQTT")
            alarmUUID = str(uuid.uuid4())
            filename = '{}_alarm_{}.h264'.format(alarmUUID, st)
            message = json.dumps({
                                    'device_id': deviceUUID,
                                    'id': alarmUUID,
                                    'place': 'office_room',
                                    'filename': filename,
                                    'type': 'detected_motion',
                                    'occurred_at': st
                                    }, sort_keys=True)
            mqttClient.publish("raspberry/main", message)
            record(FILE_DIR + filename)
            sendToMinio(filename)
            
            time.sleep(3)
        time.sleep(0.1)
        
except Exception as e:
    print(e)
    print("Quit")
    GPIO.cleanup()
    
