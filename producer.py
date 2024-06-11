#Prosty producent danych dla systemu Kafka

#Importuje klasę Producer z biblioteki confluent_kafka, która umożliwia produkcję wiadomości do Kafki
from confluent_kafka import Producer
#Importuje moduł socket, który umożliwia uzyskanie nazwy hosta (hostname) maszyny
import socket
import time
import random
import json
import csv

#Definiuje słownik conf zawierający ustawienia konfiguracyjne dla producenta Kafka. 
#Kluczami są bootstrap.servers (adresy brokerów Kafka) i client.id (identyfikator klienta), który w tym przypadku jest nazwą hosta maszyny.
conf = {'bootstrap.servers': 'kafka:9092', 'client.id': socket.gethostname()}

#src_bytes,dst_bytes,count,srv_count,dst_host_count,dst_host_srv_count,dst_host_same_srv_rate,dst_host_same_src_port_rate,attack_name
def generate_data(start_row=0):
    label_mapping = {'neptune': 1, 'normal': 0}
    with open('Nowy.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for _ in range(start_row):
            next(csvreader)
        for row in csvreader:
            X1, X2, X3, X4, X5, X6, X7, X8, X9, X10, X11 = row
            print(X11, flush=True)
            feature_11_numeric = label_mapping.get(X11, 0)
            data = {
                "feature_1": float(X1),
                "feature_2": float(X2),
                "feature_3": float(X3),
                "feature_4": float(X4),
                "feature_5": float(X5),
                "feature_6": float(X6),
                "feature_7": float(X7),
                "feature_8": float(X8),
                "feature_9": float(X9),
                "feature_10": float(X10),
                "feature_11": feature_11_numeric
            }
            return json.dumps(data)

#Tworzy obiekt producenta Kafka (Producer) zdefiniowany wcześniej konfiguracją.
p = Producer(conf)


#Główna pętla
#symuluje ciągłą generację danych i wysyłanie ich do Kafki za pomocą producenta
start_row1 = 1
while True:
    try:
        print("Producent: generuję nowe dane", flush=True)
        print(start_row1, flush=True)
        #Generuje nowe dane
        activity = generate_data(start_row1)
        start_row1 = start_row1 + 1
        #print(activity)
        #Wysyła wygenerowane dane do tematu Kafka o nazwie 'historic_data'
        p.produce('historic_data', activity)
        #Wywołuje p.flush() w celu zapewnienia wysłania wszystkich wiadomości do brokera Kafka.
        p.flush()
        #Oczekuje przez sekundę przed ponownym wykonaniem pętli.
        time.sleep(1)
    #W przypadku wystąpienia błędu, wyświetla komunikat i czeka 3 sekundy przed ponownym wykonaniem pętli.
    except Exception as e:
        print(f"Błąd: {e}")
        time.sleep(3)



