import json
import random
import time
import schedule
import paho.mqtt.client as mqtt

# 1. Definición de constantes:
#    MQTT:
#    - IP MQTT
#    - Puerto MQTT
#    - Usuario MQTT
#    - Contraseña MQTT
#    - Topic suscriptor
#    - Topic publicador
#    Configuraciones:
#    - Intervalo de medición

# 2. Definición de funciones:
#    - Función de conexión MQTT
#    - Función de publicación a tópico
#    - Función de recepción de mensajes
#    - Función de procesar mensaje
#    - Función de medición de datos
#    - Función de desconexión MQTT
#    - Función principal

'''
Dirección IP y puerto del servidor MQTT
'''
MQTT_HOST = "44.204.108.242"  # "ip.maquina.mqtt"
MQTT_PORT = 8082

'''
Usuario y contraseña para la conexión MQTT
'''
MQTT_USER = "waponte"  # "UsuarioMQTT"
MQTT_PASSWORD = "waponte"  # "ContraseñaMQTT"


'''
Topicos de suscripción y publicación
'''
BASE_TOPIC = "colombia/cundinamarca/bogota/" + \
    MQTT_USER  # "<país>/<estado>/<ciudad>/" + MQTT_USER
MQTT_PUB_TOPIC = BASE_TOPIC + "/out"
MQTT_SUB_TOPIC = BASE_TOPIC + "/in"

'''
Intervalo de medición en segundos
'''
MEASURE_INTERVAL = 2

'''
Valor medio de la temperatura en grados Celsius
que el emulador genera y la variación de la temperatura
'''
TEMPERATURE_VALUE = 28.0  # Valor elevado para disparar evento FAN_ON (original: 21.0)
TEMPERATURE_VARIATION = 3.0

'''
Valor medio de la humedad en porcentaje
que el emulador genera y la variación de la humedad
'''
MOISTURE_VALUE = 60.0
MOISTURE_VARIATION = 5.0

'''
Estado del actuador emulado (ventilador).
True = encendido, False = apagado.
'''
fan_state = False


def activate_fan(avg_1h, avg_24h):
    '''
    Emula la activación de un actuador (ventilador).
    En un dispositivo real, aquí se activaría un pin GPIO
    para encender el ventilador o un LED indicador.
    '''
    global fan_state
    fan_state = True
    print("")
    print("############################################################")
    print("#          ACTUADOR: VENTILADOR ENCENDIDO                   #")
    print("#          Temperatura reciente: {:<26}#".format("{:.2f}°C".format(avg_1h)))
    print("#          Promedio 24h:         {:<26}#".format("{:.2f}°C".format(avg_24h)))
    print("#          Motivo: Calentamiento anómalo detectado          #")
    print("############################################################")
    print("")


def deactivate_fan(avg_1h, avg_24h):
    '''
    Emula la desactivación del actuador (ventilador).
    En un dispositivo real, aquí se desactivaría el pin GPIO.
    '''
    global fan_state
    fan_state = False
    print("")
    print("############################################################")
    print("#          ACTUADOR: VENTILADOR APAGADO                     #")
    print("#          Temperatura reciente: {:<26}#".format("{:.2f}°C".format(avg_1h)))
    print("#          Promedio 24h:         {:<26}#".format("{:.2f}°C".format(avg_24h)))
    print("#          Motivo: Temperatura normal                       #")
    print("############################################################")
    print("")


def process_message(msg: str):
    '''
    Procesar mensaje recibido.
    Maneja dos tipos de mensajes:
      - ALERT: Alerta de valores fuera de rango (existente)
      - EVENT: Comando de actuador basado en análisis de eventos (nuevo)
    '''
    print("Procesando mensaje: " + msg)

    if msg.startswith("ALERT"):
        print("############################################################")
        print("############################################################")
        print("############################################################")
        print("              ALERTA: {}                     ".format(msg))
        print("############################################################")
        print("############################################################")
        print("############################################################")

    elif msg.startswith("EVENT"):
        parts = msg.split()
        # Formato: EVENT FAN_ON|FAN_OFF avg_1h avg_24h
        if len(parts) >= 4:
            action = parts[1]
            avg_1h = float(parts[2])
            avg_24h = float(parts[3])

            if action == "FAN_ON":
                activate_fan(avg_1h, avg_24h)
            elif action == "FAN_OFF":
                deactivate_fan(avg_1h, avg_24h)
            else:
                print("Acción de evento desconocida: " + action)
        else:
            print("Formato de evento inválido: " + msg)


def mqtt_publish(topic, msg):
    '''
    Publicar mensaje a tópico
    '''
    client.publish(topic, msg)


def measure_temperature():
    '''
    Función de medición de temperatura
    En emulación, estos datos son aleatorios con distribución uniforme
    desde el valor medio -variación hasta el valor medio +variación.
    Si se utilizara un sensor, acá se debería leer la temperatura real.
    '''
    min_value = TEMPERATURE_VALUE - TEMPERATURE_VARIATION
    max_value = TEMPERATURE_VALUE + TEMPERATURE_VARIATION
    return random.uniform(min_value, max_value)


def measure_moisture():
    '''
    Función de medición de humedad
    En emulación, estos datos son aleatorios con distribución uniforme
    desde el valor medio -variación hasta el valor medio +variación.
    Si se utilizara un sensor, acá se debería leer la humedad real.
    '''
    min_value = MOISTURE_VALUE - MOISTURE_VARIATION
    max_value = MOISTURE_VALUE + MOISTURE_VARIATION
    return random.uniform(min_value, max_value)


def on_connect(client, userdata, connect_flags, reason_code, properties):
    '''
    Función de conexión MQTT
    '''
    print("Connected with result: " + str(reason_code))
    client.subscribe(MQTT_SUB_TOPIC)


def on_message(client, userdata, msg):
    '''
    Función de recepción de mensajes
    '''
    data = msg.payload.decode("utf-8")
    print(msg.topic + ": " + str(data))
    process_message(data)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    '''
    Función de desconexión MQTT
    '''
    print("Disconnected with result: " + str(reason_code))


def mqtt_setup():
    '''
    Función de conexión MQTT
    '''
    global client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, MQTT_USER)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.loop_start()


def measure_data():
    '''
    Función de medición y envío de datos.
    Muestra además el estado actual del actuador (ventilador emulado).
    '''
    print("Midiendo...")
    temperature = measure_temperature()
    moisture = measure_moisture()
    print("\tTemperatura: {:.2f}°C".format(temperature))
    print("\tHumedad: {:.2f}%".format(moisture))
    print("\tVentilador: {}".format("ENCENDIDO" if fan_state else "APAGADO"))
    mqtt_publish(MQTT_PUB_TOPIC, json.dumps({
        "temperatura": temperature,
        "humedad": moisture
    }))
    print("Datos enviados")


def start_measurement():
    '''
    Función que ejecuta cada intervalo de tiempo la medición de datos
    '''
    schedule.every(MEASURE_INTERVAL).seconds.do(measure_data)
    while True:
        schedule.run_pending()
        time.sleep(1)


client = None

mqtt_setup()
start_measurement()
