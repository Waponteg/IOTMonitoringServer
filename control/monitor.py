from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta
from django.utils import timezone
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, settings.MQTT_USER_PUB)


def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    data = Data.objects.filter(
        base_time__gte=timezone.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alert = True

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(timezone.now(), "Sending alert to {} {}".format(topic, variable))
            client.publish(topic, message)
            alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")


# Umbral de desviación de temperatura (°C) entre el promedio de 1 hora vs 24 horas.
# Si la temperatura reciente supera al promedio histórico por este valor, se activa el ventilador.
DEVIATION_THRESHOLD = 2.0


def analyze_events():
    """
    Nuevo procesamiento de evento basado en consulta a la base de datos.

    Pre-requisito (consulta a BD): Se calcula el promedio de temperatura
    de las últimas 24 horas por estación.

    Condición: Si el promedio de temperatura de la última hora supera
    al promedio de las últimas 24 horas por más de DEVIATION_THRESHOLD °C,
    se detecta una tendencia de calentamiento anómalo.

    Acción: Se envía un comando al dispositivo IoT para activar o desactivar
    un actuador (ventilador emulado) vía MQTT.
    """
    print("Analizando eventos de temperatura...")

    # ---- PRE-REQUISITO: Consulta a la base de datos ----
    # Consulta 1: Promedio de temperatura de las últimas 24 horas por estación
    data_24h = Data.objects.filter(
        base_time__gte=timezone.now() - timedelta(hours=24),
        measurement__name="temperatura"
    ).values(
        'station__user__username',
        'station__location__city__name',
        'station__location__state__name',
        'station__location__country__name'
    ).annotate(avg_temp_24h=Avg('avg_value'))

    # Consulta 2: Promedio de temperatura de la última hora por estación
    data_1h = Data.objects.filter(
        base_time__gte=timezone.now() - timedelta(hours=1),
        measurement__name="temperatura"
    ).values(
        'station__user__username',
        'station__location__city__name',
        'station__location__state__name',
        'station__location__country__name'
    ).annotate(avg_temp_1h=Avg('avg_value'))

    # Indexar promedios de 24h por usuario para búsqueda eficiente
    avg_24h_by_user = {}
    for item in data_24h:
        avg_24h_by_user[item['station__user__username']] = item['avg_temp_24h']

    events = 0
    for station_1h in data_1h:
        user = station_1h['station__user__username']
        avg_1h = station_1h['avg_temp_1h']

        # Si no hay datos históricos de 24h para esta estación, se omite
        if user not in avg_24h_by_user:
            continue

        avg_24h = avg_24h_by_user[user]

        country = station_1h['station__location__country__name']
        state = station_1h['station__location__state__name']
        city = station_1h['station__location__city__name']
        topic = '{}/{}/{}/{}/in'.format(country, state, city, user)

        # ---- CONDICIÓN: Evaluar desviación ----
        if avg_1h > avg_24h + DEVIATION_THRESHOLD:
            # La temperatura reciente supera significativamente al promedio histórico
            message = "EVENT FAN_ON {:.2f} {:.2f}".format(avg_1h, avg_24h)
            print(timezone.now(),
                  "Activando ventilador para {}: temp_1h={:.2f} > avg_24h={:.2f} + {}"
                  .format(user, avg_1h, avg_24h, DEVIATION_THRESHOLD))
            client.publish(topic, message)
            events += 1
        elif avg_1h <= avg_24h:
            # La temperatura reciente es normal o menor al promedio histórico
            message = "EVENT FAN_OFF {:.2f} {:.2f}".format(avg_1h, avg_24h)
            print(timezone.now(),
                  "Desactivando ventilador para {}: temp_1h={:.2f} <= avg_24h={:.2f}"
                  .format(user, avg_1h, avg_24h))
            client.publish(topic, message)
            events += 1

    print("{} eventos procesados".format(events))


def on_connect(client, userdata, connect_flags, reason_code, properties):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", str(reason_code))


def on_disconnect(client: mqtt.Client, userdata, disconnect_flags, reason_code, properties):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(reason_code))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''

    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos
    y la función analyze_events cada 5 minutos para el procesamiento de eventos.
    '''
    print("Iniciando cron...")
    schedule.every(5).minutes.do(analyze_data)
    schedule.every(5).minutes.do(analyze_events)
    print("Servicio de control iniciado (alertas + eventos)")
    while 1:
        schedule.run_pending()
        time.sleep(1)
