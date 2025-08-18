from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/message')
def get_message():
    data = {'message': '¡Hola desde el servidor backend de Flask!'}
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
