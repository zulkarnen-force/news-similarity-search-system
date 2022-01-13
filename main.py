from amqpstorm import Connection
from consume import on_message

connection = Connection('128.199.145.214', 'bino', 'binotest0')
channel = connection.channel()
channel.basic.consume(callback=on_message, queue='files', no_ack=False)
channel.start_consuming(to_tuple=False)