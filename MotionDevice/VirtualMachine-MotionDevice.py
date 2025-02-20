from azure.iot.device import IoTHubDeviceClient, Message
import random
import time
import json


# Replace the string with Actual Motion device Connection String 
connection_string = "Enter MotionDevice Connection String "

# Extract the device ID from the connection string
device_id = connection_string.split(";")[1].split("=")[1]

# Create a client using the device connection string
client = IoTHubDeviceClient.create_from_connection_string(connection_string)

def send_telemetry(record_count):
    # Simulate telemetry data: RPM, vibration, and motion detection
    telemetry = {
        "MessageID": int(record_count),  # Message ID
        "DeviceID": str(device_id),  # Device ID
        "RPM": int(random.uniform(2000, 5500)),  # Random RPM between 1000 and 5000
        "Vibration": int(random.uniform(1, 10)),  # Random vibration level between 0.1 and 5.0
        "MotionDetected": random.choice([True, False]),  # Random motion detection (True/False)
        "DeviceStatus": random.choice(["Normal", "Warning", "Critical"]),# Device status (Normal, Warning, Critical)
        "Temperature" : int( random.uniform(80 ,150))
    }

    # Create a message from the telemetry data, ensuring it's properly serialized to JSON
    message = Message(json.dumps(telemetry))  # Use json.dumps to convert dictionary to JSON string
    
    # Send the message to IoT Hub
    client.send_message(message)
    print(f"Message {record_count} sent: {telemetry}")


try:
    record_count = 0  # Initialize record counter
    while record_count < 20:  # Stop after 20 records
        record_count += 1
        send_telemetry(record_count)  # Send telemetry data
        time.sleep(5)  # Delay for 5 seconds before sending the next message
    print("Telemetry simulation completed. 10 records sent.")
except KeyboardInterrupt:
    print("Simulation stopped manually.")
finally:
    # Disconnect the client
    client.shutdown()
    print("Client disconnected.")

