import time
import pika, sys, os
import random
class Rabbit:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='message_handler')
        self.channel.queue_declare(queue='build')        #self.channel.exchange_declare(exchange='direct')
        #self.channel.queue_bind(exchange='direct', queue='build' , routing_key='build')
        self.response_list = []

    
    def callback(self, ch, method, properties, body):
        self.response_list.append(body.decode())
        print(f"Response : {body.decode()}")
        #print("-----------------------")
    
    def receive(self):
        self.response_list.clear()
        self.channel.basic_consume(queue='build',
                        on_message_callback=self.callback,
                        auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')

        with self.channel.connection._acquire_event_dispatch() as dispatch_allowed:
                if not dispatch_allowed:
                    raise self.channel.exceptions.ReentrancyError(
                        'start_consuming may not be called from the scope of '
                        'another BlockingConnection or BlockingChannel callback')

        self.channel._impl._raise_if_not_open()
        msg_counter = 0
        max_msg = 1
        while msg_counter < max_msg:
            #print("hello me %d",msg_counter)
            self.channel._process_data_events(time_limit=None)
            #max_msg = int(self.response_list[0].split()[0])
            msg_counter += 1
        
        return self.response_list.copy()
    def send(self,msg:str):
        self.channel.basic_publish(exchange='',
                      routing_key='message_handler',
                      body=msg)

        #print("message Sent")

        
    
        

def main():
    rabbit = Rabbit()
    while True:
        responses = rabbit.receive()
        if len(responses) > 0:
            order = responses[0]
            rand_int = random.randint(0,1)
            if(rand_int == 0):
                rabbit.send("builder: Not building "+order)
            else:
                rabbit.send("builder: Building " + order)
        responses.clear()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)