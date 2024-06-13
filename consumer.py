#część systemu przetwarzania strumieniowego danych, który odczytuje dane z tematu Kafka, 
#przetwarza je, szkoli model, a następnie ocenia wydajność tego modelu na zbiorze testowym

#Importuje confluent_kafka do obsługi komunikacji z Kafka
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
import socket
import time
import json
#pandas do przetwarzania danych
import pandas as pd
#dask do rozproszonego przetwarzania danych
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
from dask_ml.linear_model import LinearRegression, LogisticRegression

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, explained_variance_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, log_loss

from sklearn.preprocessing import label_binarize
import numpy as np
import os

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'client.id': socket.gethostname()
}

#pobiera najnowsze dostępne offsety dla wszystkich partycji określonego tematu w Kafka
#co pozwala na monitorowanie, jakie wiadomości są najnowsze w danym temacie.
def get_latest_offsets(consumer, topic):
    #Inicjalizacja słownika latest_offsets, będzie przechowywał najnowsze offsety dla każdej partycji
    latest_offsets = {}
    #pobiera listę tematów dostępnych w Kafka, a następnie wybiera temat podany jako argument (topic)
    partitions = consumer.list_topics(topic).topics[topic].partitions
    #Iteracja przez partycje: #iteruje przez wszystkie partycje tematu.
    for partition in partitions:
        #Dla każdej partycji tworzony jest obiekt TopicPartition, który łączy temat z partycją.
        tp = TopicPartition(topic, partition)
        #pobiera zakres offsetów (najniższy i najwyższy) dla danej partycji (tp).
        #low to najniższy offset (pierwsza dostępna wiadomość).
        #high to najwyższy offset (najnowsza wiadomość).
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        #Przypisanie wysokiego offsetu do partycji: Najwyższy offset (high) jest przypisywany do odpowiedniej partycji w słowniku latest_offsets.
        latest_offsets[partition] = high
    #Zwracanie słownika z najnowszymi offsetami:
    return latest_offsets


#Funkcja consume_messages_to_latest jest odpowiedzialna za konsumowanie wiadomości 
#z topiku Kafka do momentu osiągnięcia najnowszego offsetu w każdej partycji. 
def consume_messages_to_latest():
    #Tworzy instancję konsumenta Kafka za pomocą dostarczonej konfiguracji conf.
    c = Consumer(conf)
    #Subskrybuje konsumenta do określonego topiku.
    topic = 'historic_data'
    c.subscribe([topic])
    #Inicjalizuje pustą listę do przechowywania skonsumowanych wiadomości.
    messages = []

    try:
        #pobrać najnowsze offsety dla wszystkich partycji w topiku.
        latest_offsets = get_latest_offsets(c, topic)
        
        #Tworzy listę obiektów TopicPartition dla każdej partycji z offsetem początkowym równym 0.
        partitions = [TopicPartition(topic, p, 0) for p in latest_offsets]
        #Przypisuje konsumenta do określonych partycji.
        c.assign(partitions)
        
        #Rozpoczyna nieskończoną pętlę do konsumowania wiadomości.
        while True:
            #Pobiera wiadomość z topiku z czasem oczekiwania 1 sekundy.
            msg = c.poll(timeout=1.0)
            #Jeśli nie ma wiadomości, przechodzi do następnej iteracji.
            if msg is None:
                continue
            #Sprawdza, czy wiadomość zawiera błąd.
            if msg.error():
                #Jeśli kod błędu to _PARTITION_EOF, oznacza to koniec partycji, więc przechodzi do następnej iteracji.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    #W przeciwnym razie, rzuca wyjątek KafkaException.
                    raise KafkaException(msg.error())
            
            #Pobiera numer partycji wiadomości.
            partition = msg.partition()
            #Pobiera offset wiadomości.
            offset = msg.offset()
            #Dekoduje i dodaje wiadomość do listy messages.
            messages.append(json.loads(msg.value().decode('utf-8')))

            #Sprawdza, czy obecny offset jest równy najnowszemu offsetowi minus 1.
            if offset == latest_offsets[partition] - 1:
                #suwa partycję z listy latest_offsets, jeśli osiągnięto najnowszy offset.
                del latest_offsets[partition]
            #Jeśli lista latest_offsets jest pusta, przerywa pętlę.
            if not latest_offsets:
                break
    #Obsługuje wszelkie wyjątki, drukując komunikat o błędzie.
    except Exception as e:
        print(f"Błąd: {e}")
    #Zamyka konsumenta niezależnie od wyniku operacji.
    finally:
        c.close()
    
    #Zwraca listę skonsumowanych wiadomości.
    return messages


#Budowanie modelu REGRESJI LINIOWEJ
def build_model_energy():
    try:
        messages = consume_messages_to_latest()
    except Exception as e:
        print(f"Błąd: {e}")

    #Dane z Kafki są najpierw wczytywane do Pandas DataFrame,
    df = pd.DataFrame(messages)
    # a następnie konwertowane do Dask DataFrame przy użyciu dd.from_pandas().
    # Dask DataFrame umożliwia rozproszone przetwarzanie dużych zbiorów danych,
    df = dd.from_pandas(pd.DataFrame(df), npartitions=8)

    # Wybieramy kolumny do X i y
    X = df[['feature_3', 'feature_4', 'feature_5', 'feature_6', 'feature_7', 'feature_8', 'feature_9', 'feature_10', 'feature_11', 'feature_12', 'feature_13', 'feature_14', 'feature_15', 'feature_16']]
    y = df['feature_2']

    # Dzielimy dane na zestawy treningowe i testowe
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    #Inicjalizujemy modelu regresji liniowej
    clf = LinearRegression()

    # Trenujemy model
    clf.fit(X_train.values.compute(), y_train.values.compute())

    # Przewidujemy wartości
    y_pred = clf.predict(X_test.values.compute())

    # Obliczamy metryki regresji
    mae = mean_absolute_error(y_test.compute(), y_pred)
    mse = mean_squared_error(y_test.compute(), y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test.compute(), y_pred)
    explained_variance = explained_variance_score(y_test.compute(), y_pred)
    mape = np.mean(np.abs((y_test.compute() - y_pred) / y_test.compute())) * 100

    print(f"Mean Absolute Error: {mae}")
    print(f"Mean Squared Error: {mse}")
    print(f"Root Mean Squared Error: {rmse}")
    print(f"R-squared: {r2}")
    print(f"Explained Variance: {explained_variance}")
    print(f"Mean Absolute Percentage Error: {mape}")
    print(f"Ilość odebranych wiadomości: {len(df)}")



#Budowanie modelu KLASYFIKACJI
def build_model_attack():
    try:
        messages = consume_messages_to_latest()
    except Exception as e:
        print(f"Błąd: {e}")

    #Dane z Kafki są najpierw wczytywane do Pandas DataFrame,
    df = pd.DataFrame(messages)
    # a następnie konwertowane do Dask DataFrame przy użyciu dd.from_pandas().
    # Dask DataFrame umożliwia rozproszone przetwarzanie dużych zbiorów danych,
    df = dd.from_pandas(pd.DataFrame(df), npartitions=8)

    # Wybieramy kolumny do X i y
    X = df[['feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5', 'feature_6', 'feature_7', 'feature_8', 'feature_9', 'feature_10']]
    y = df['feature_11']

    # Dzielimy dane na zestawy treningowe i testowe
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, shuffle=False)

    # Inicjalizujemy model klasyfikacyjny
    clf = LogisticRegression()

    # Trenujemy model
    clf.fit(X_train.values.compute(), y_train.values.compute())

    # Użycie decision_function do uzyskania odległości od hiperpowierzchni decyzyjnej
    clf.decision_function(X_train.values.compute())

    # Przewidujemy wartości
    y_pred = clf.predict(X_test.values.compute())

    # Prawdopodobieństwa dla klas (potrzebne do obliczenia AUC i Log Loss)
    y_prob = clf.predict_proba(X_test.values.compute())



    # Accuracy
    accuracy = accuracy_score(y_test.compute(), y_pred)
    # Precision
    precision = precision_score(y_test.compute(), y_pred)
    # Recall
    recall = recall_score(y_test.compute(), y_pred)
    # F1 Score
    f1 = f1_score(y_test.compute(), y_pred)
    # Log Loss
    logloss = log_loss(y_test.compute(), y_prob)

    # Obliczamy metryki regresji
    mae = mean_absolute_error(y_test.values.compute(), y_pred)
    mse = mean_squared_error(y_test.values.compute(), y_pred)
    rmse = np.sqrt(mse)

    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1 Score: {f1}")
    print(f"Log Loss: {logloss}")

    print(f"Mean Absolute Error: {mae}")
    print(f"Mean Squared Error: {mse}")
    print(f"Root Mean Squared Error: {rmse}")
    print(f"Ilość odebranych wiadomości: {len(df)}")



#PROGRAM GŁÓWNY
if (os.getenv('MODEL_ENERGY', 'false').lower() == 'true'):
    build_model_energy()

elif (os.getenv('MODEL_ATTACK', 'false').lower() == 'true'):
    build_model_attack()