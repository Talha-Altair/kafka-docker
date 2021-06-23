from confluent_kafka import Producer
import json

def startpy():

    p = Producer({'bootstrap.servers': 'localhost:9091'})

    p.produce('project-x', key='1', value=json.dumps({"Talha":'Altair'}))

    p.flush(30)

if __name__ == '__main__':
    startpy()