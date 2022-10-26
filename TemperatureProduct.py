from datetime import timedelta, datetime
from time import sleep 
import boto3
import json
import random


cliente = boto3.client("kinesis", aws_access_key_id="????", aws_secret_access_key="????",
                   region_name="us-east-1")

count = 0
id = "PT_"+str(count)
time_ref = datetime.now()

while True:
    temperature = random.randint(20,25)
    registro = {"id":id, 'data':temperature, "type": "temperature", 'timestamp':time_ref.strftime("%d/%m/%Y")}
    print(registro)

    response = cliente.put_record(
                    StreamName = "stream1",
                    Data = json.dumps(registro),
                    PartitionKey="02"
               )

    print(response)           

    count+=1
    id =  "PT_"+str(count)
    time_ref = time_ref + timedelta(1)
    sleep(60)
