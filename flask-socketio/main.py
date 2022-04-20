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


@socketio.on('request')
def handle_request(request: dict):
    """this function for handle similarity request

    Args:
        request (dict): {column_name, text, filename, similarity, cell}
            Example 
            {
                column_name: "content", text: "foo and bar", filename: "bino_23234.xlsx", cell: "A1"
            }
    """
    

    try :
        
        result = similarity_word(request)
        emit('response', result)

    except BaseException as err:
        console.error('From: {}: \n {}'.format(__file__, err))
        emit('error', err.args)
        return False;


@socketio.on('message')
def handleMessage(msg):
    send(f'FROM SERVER: {msg}', broadcast=True)
    msg = msg.split(" ; ")
    msg = np.array(msg)
    
    # similarity_word(str(msg[0]),str(msg[1]),str(msg[2]))
    # similarity_word_preproses(str(msg[0]),str(msg[1]),str(msg[2]))
    
if __name__ == '__main__':
    socketio.run(app)