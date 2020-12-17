import time
import json
import datetime
import RPi.GPIO as GPIO
import picamera
from minio import Minio
from minio.error import S3Error

import paho.mqtt.client as mqtt

GPIO.setmode(GPIO.BCM)
pirPin = 26
camera = picamera.PiCamera()
GPIO.setup(pirPin, GPIO.IN)

brokerAddress = "192.168.0.17"
mqttClient = mqtt.Client("P1")
mqttClient.connect(brokerAddress)

imageDirectory = 'snapshots/'
client = Minio(
        "192.168.0.17:443",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
bucketName = 'raspberrycamera'
found = client.bucket_exists(bucketName)
if not found:
    client.make_bucket(bucketName)
else:
    print("Bucket 'raspberry_camera' already exists")


def lights():
    print("Motion Detected!")
    time.sleep(2)

print('Motion sensor alarm')
time.sleep(2)
print('Ready')

try:
    while True:
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%d.%m.%Y %H:%M:%S')
        if GPIO.input(pirPin):
            time.sleep(0.5)
            print("Motion Detected... Sending to MQTT")
            message = json.dumps({'place': 'office_room', 'alarm_type': 'detected_motion', 'occured_at': st}, sort_keys=True)
            mqttClient.publish("raspberry/main", message)
            imageName = 'alarm_{}.jpg'.format(st)
            camera.capture(imageDirectory + imageName)
            client.fput_object(
                bucketName, "imageName", imageDirectory + imageName,
            )
            time.sleep(3)
        time.sleep(0.1)
        
except Exception as e:
    print(e)
    print("Quit")
    GPIO.cleanup()
    
