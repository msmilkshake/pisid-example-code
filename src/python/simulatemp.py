#py -m pip install paho-mqtt
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from datetime import datetime
import time

from getpass import getpass
def on_connectMqttTemp(client, userdata, flags, rc):
    print("MQTT Temperature Connected with result code "+str(rc))

topic="test_topic"
clientMqttMovements = mqtt.Client()
clientMqttMovements.on_connect = on_connectMqttTemp
clientMqttMovements.connect('localhost', 1883)
i=0
while True:
    i = i+1            
    if (i==50):
        i=-50
    try: 
        mensagem =   "{Hora: \"" + str(datetime.now()) + "\", Leitura: " + str(i) + ", Sensor: 1}" 
        print(mensagem)       
        clientMqttMovements.publish(topic,mensagem,qos=0)
        clientMqttMovements.loop()
        mensagem =   "{Hora: \"" + str(datetime.now()) + "\", Leitura: " + str(i+1) + ", Sensor: 2}" 
        print(mensagem) 
        clientMqttMovements.publish(topic,mensagem,qos=2)
        clientMqttMovements.loop()       
        time.sleep(1) 
    except Exception:
        print("Error sendMqtt")
        pass   
      