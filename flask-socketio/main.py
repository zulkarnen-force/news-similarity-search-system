from flask import Flask, render_template
from flask_socketio import SocketIO, send, emit
import json
import numpy as np
from similarity_preprosess import similarity_word_preproses
from similarity import  similarity_word
from py_console import console

app = Flask(__name__,template_folder='../flask-socketio/templates')
app.config['SECRET_KEY'] = 'mysecret'
socketio = SocketIO(app, cors_allowed_origins='*')


@app.route('/')
def index():
    return render_template('index.html')


@socketio.on('request-similarity')
def handle_request(request: dict):
    """this function for handle similarity request

    Args:
        request (dict): {column_name, text, filename, similarity}
    """
    
    column_name, text, filename, similarity = \
    request['column_name'], request['text'], request['filename'], request['similarity']
    
    try :
        
        result = similarity_word(column_name, text, filename, float(similarity))
        emit('response-similarity', result)

    except Exception as e:
        console.error(e)
        emit('error', e.args)
        return False;


@socketio.on('message')
def handleMessage(msg):
    send(msg, broadcast=True)
    msg = msg.split(" ; ")
    msg = np.array(msg)
    
    # similarity_word(str(msg[0]),str(msg[1]),str(msg[2]))
    # similarity_word_preproses(str(msg[0]),str(msg[1]),str(msg[2]))
    
if __name__ == '__main__':
    socketio.run(app)