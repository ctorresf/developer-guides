import os
import datetime
from flask import Flask, jsonify

# Crea la instancia de la aplicación Flask.
app = Flask(__name__)

# Rutas de los archivos de configuración y log.
# La ruta 'config/' se mapea a 'app-config' con un bind mount.
CONFIG_FILE_PATH = 'config/message.txt'
# La ruta 'logs/' se mapea al volumen con nombre.
LOG_FILE_PATH = 'logs/app.log'

@app.route('/')
def home():
    """
    Ruta principal que lee un mensaje de un archivo y escribe una entrada en un archivo de log.
    """
    # Se asegura de que el directorio de logs exista.
    log_dir = os.path.dirname(LOG_FILE_PATH)
    os.makedirs(log_dir, exist_ok=True)

    # Lee el mensaje del archivo montado (bind mount).
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            message_from_file = f.read().strip()
    except FileNotFoundError:
        message_from_file = f"Error: Archivo de configuración '{CONFIG_FILE_PATH}' no encontrado."

    # Escribe una entrada en el log (en el volumen con nombre).
    with open(LOG_FILE_PATH, 'a') as f:
        log_entry = f"[{datetime.datetime.now()}] Acceso a la página principal. Mensaje del archivo: '{message_from_file}'\n"
        f.write(log_entry)

    # Lee el contenido del archivo de logs.
    with open(LOG_FILE_PATH, 'r') as file:
        lines = file.readlines()

    # Devuelve una respuesta JSON.
    return jsonify({
        "status": "success",
        "message_read_from_file": message_from_file,
        "log_entry_written_to_file": log_entry.strip(),
        "log_contents": lines
    })

# Punto de entrada para la aplicación.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8010)
