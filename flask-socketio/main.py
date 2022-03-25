from flask import Flask, render_template
from flask_socketio import SocketIO, send
import json
import numpy as np
# from similarity import similarity_word
from similarity_preprosess import similarity_word_preproses

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mysecret'
socketio = SocketIO(app, cors_allowed_origins='*')
app = Flask(__name__, template_folder='../flask-socketio')

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('message')
def handleMessage(msg):
    send(msg, broadcast=True)
    msg = msg.split(" ; ")
    msg = np.array(msg)
    
    # similarity_word(str(msg[0]),str(msg[1]),str(msg[2]))
    similarity_word_preproses(str(msg[0]),str(msg[1]),str(msg[2]))
    
if __name__ == '__main__':
	socketio.run(app)