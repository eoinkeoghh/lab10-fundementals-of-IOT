# Lab 10 – MQTT + Protocol Buffers (Built-in LED Only)
# Niamh Flynn (C22388461), Eoin Keogh (C22456452), Cormac Holohan (C22363913)

import network
import time
from machine import ADC, Pin, RTC
from umqtt.simple import MQTTClient
import ubinascii
import machine

SSID = "Eoin’s iPhone "
PASSWORD = "hotspot12"
BROKER_IP = "172.20.10.3"
PORT = 8080
TOPIC = b"temp/pico"

# ROLE CONFIGURATION
OUTPUT_PIN = None      # None for publisher, "LED" (or GPIO number) for subscriber
PUB_IDENT = 1          # INT for publisher (1,2,3...), None for subscriber

# ---- Import YOUR generated protobuf module ----
import temp_message_upb2 as pb


# ---------- Role Validation ----------
is_publisher  = (PUB_IDENT is not None) and (OUTPUT_PIN is None)
is_subscriber = (OUTPUT_PIN is not None) and (PUB_IDENT is None)

if not (is_publisher or is_subscriber):
    print("CONFIG ERROR:")
    print("Publisher requires: PUB_IDENT != None AND OUTPUT_PIN == None")
    print("Subscriber requires: OUTPUT_PIN != None AND PUB_IDENT == None")
    print("Current values:", PUB_IDENT, OUTPUT_PIN)
    raise SystemExit


# ---------- WiFi Connect ----------
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(SSID, PASSWORD)
print("Connecting to Wi-Fi ...")
while not wlan.isconnected():
    time.sleep(1)
print("Connected:", wlan.ifconfig())


# ---------- MQTT Setup ----------
if is_publisher:
    client_id = b"publisher_" + str(PUB_IDENT).encode()
else:
    client_id = b"subscriber_" + ubinascii.hexlify(machine.unique_id())

mqtt = MQTTClient(client_id=client_id, server=BROKER_IP, port=PORT, keepalive=7000)
mqtt.connect()
print("Connected to MQTT broker at", BROKER_IP)


#                    PUBLISHER MODE
if is_publisher:
    sensor_temp = ADC(4)
    conversion_factor = 3.3 / 65535
    rtc = RTC()

    while True:
        # --- Read temperature ---
        reading = sensor_temp.read_u16() * conversion_factor
        temp_c = 27 - (reading - 0.706) / 0.001721

        # --- Get RTC time ---
        dt = rtc.datetime()
        h, m, s = dt[4], dt[5], dt[6]

        # --- Build protobuf message ---
        message = pb.SensormessageMessage()
        message.temperature = float(temp_c)
        message.publisher_id = int(PUB_IDENT)

        message.time = time.time()

        # --- Serialize + publish ---
        payload = message.serialize()
        mqtt.publish(TOPIC, payload)

        print(f"Published from {PUB_IDENT}: {temp_c:.2f} °C at {h:02d}:{m:02d}:{s:02d}")
        time.sleep(0.5)

#                    SUBSCRIBER MODE
else:
    rtc = RTC()
    led = Pin(OUTPUT_PIN, Pin.OUT)

    # {pub_id: (temp, msg_seconds)}
    latest_temps = {}

    def to_seconds(hour, minute, second):
        return hour * 3600 + minute * 60 + second

    def callback(topic, msg):
        try:
            # --- Parse protobuf bytes ---
            m = pb.SensormessageMessage()
            m.parse(msg)

            temp = m.temperature._value
            pub_id = m.publisher_id._value

            msg_seconds = m.time._value
            latest_temps[pub_id] = (temp, msg_seconds)

            print(f"Received from {pub_id}: {temp:.2f} °C at {msg_seconds}")

        except Exception as e:
            print("Error decoding protobuf:", e)


    mqtt.set_callback(callback)
    mqtt.subscribe(TOPIC)
    print("Subscribed to", TOPIC)


    def compute_average():
        now = rtc.datetime()
        now_seconds = to_seconds(now[4], now[5], now[6])

        temps = []
        stale = []

        for pub_id, (temp, msg_seconds) in latest_temps.items():
            diff = now_seconds - msg_seconds
            if diff < 0:
                diff += 24 * 3600  # handle midnight wrap

            if diff <= 600:  # 10 minutes
                temps.append(temp)
            else:
                stale.append(pub_id)

        # remove stale publishers
        for sid in stale:
            del latest_temps[sid]
            print("Removed stale publisher:", sid)

        if temps:
            return sum(temps) / len(temps)
        return None


    while True:
        mqtt.check_msg()
        avg_temp = compute_average()

        if avg_temp is not None:
            print(f"Average temperature: {avg_temp:.2f} °C")
            led.value(1 if avg_temp > 25 else 0)

        time.sleep(0.5)
