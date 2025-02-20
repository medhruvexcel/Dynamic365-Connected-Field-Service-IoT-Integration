from azure.iot.device import IoTHubDeviceClient, Message
import random
import time
import json 

# Your IoT Hub Device Connection String
# Replace with your actual device connection string
connection_string = "Enter Thermostat Device Connection String "

# Extract the device ID from the connection string
device_id = connection_string.split(";")[1].split("=")[1]

# Create a client using the device connection string
client = IoTHubDeviceClient.create_from_connection_string(connection_string)

def send_telemetry(record_count):
    # Simulate telemetry data: temperature and humidity
    telemetry = {
        "messageId": int(record_count),  # Message ID
        "deviceId": str(device_id) , # Device ID
        "temperature": round(float(random.uniform(50, 80)), 2),  # Random temperature
        "humidity": round(float(random.uniform(40, 90)), 2)  # Random humidity
        
    }
    
    # Create a message from the telemetry data
    message = Message(str(telemetry))
    
    # Send the message to IoT Hub
    client.send_message(message)
    print(f"Message {record_count} sent: {telemetry}")

try:
    record_count = 0  # Initialize record counter
    while record_count < 20:  # Stop after 20 records
        record_count += 1
        send_telemetry(record_count)  # Send telemetry data
        time.sleep(5)  # Delay for 5 seconds before sending the next message
    print("Telemetry simulation completed. 20 records sent.")
except KeyboardInterrupt:
    print("Simulation stopped manually.")
finally:
    # Disconnect the client
    client.shutdown()
    print("Client disconnected.")
