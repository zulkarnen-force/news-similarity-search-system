from msilib.schema import ControlEvent
from amqpstorm import Connection
from consume import on_message
import env


connection = Connection(env.HOST, env.username, env.password)
channel = connection.channel()
channel.basic.consume(callback=on_message, queue=env.queue, no_ack=False)
print(f' [ OK ] Start consuming on queue name {env.queue}...')
channel.start_consuming(to_tuple=False)