import time
import pika, sys, os

import time
import pika, sys, os

class Rabbit:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='message_handler')
        self.channel.queue_declare(queue='build')
        self.channel.queue_declare(queue="backend")
        self.channel.queue_declare(queue="runner")
        #self.channel.exchange_declare(exchange='direct')
        #self.channel.queue_bind(exchange='direct', queue='message_handler' , routing_key='message_handler')
        #self.channel.queue_bind(exchange='direct', queue='build' , routing_key='build')

        self.response_list = []

    
    def callback(self, ch, method, properties, body):
        self.response_list.append(body.decode())
        #print(f"Response : {body.decode()}")
    
    def receive(self,route:str):
        self.response_list.clear()
        self.channel.basic_consume(queue=route,
                        on_message_callback=self.callback,
                        auto_ack=True)

        print(f'Waiting messages from {route}. To exit press CTRL+C')
        print(f"messages are {self.response_list}")
        
        msg_counter = 0
        max_msg = 10
        timer = time.time()
        while msg_counter < max_msg and time.time() - timer < 10:
            #print("hello me %d",msg_counter)
            self.channel._process_data_events(time_limit=10)
            #max_msg = int(self.response_list[0].split()[0])
            msg_counter += 1
        
        print(f"messages are {self.response_list}")
        return self.response_list.copy()
    
    def send(self,msg:str,route:str):
        print(f"sending {msg} to {route}")
        self.channel.basic_publish(exchange='',
                      routing_key=route,
                      body=msg)

        ##print("-----------------------")

        
    
        

def main():
    rabbit = Rabbit()
    order = ""
    while True:
        responses = rabbit.receive("message_handler")
        print("******************************")
        print("these messages are recieved")
        print(responses)
        print("********************************")
        for i in responses:
            if i.startswith("back"):
                if "build" in i:
                    build_order = i[6:]
                    rabbit.send(build_order,"build")
                elif "run" in i:
                    build_order = i[6:]
                    rabbit.send(build_order,"runner")    
            elif i.startswith("builder"):
                order = i[9:]
                rabbit.send(order,"backend")
            elif i.startswith("runner"):
                order = i[8:]
                rabbit.send(order,"backend")
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