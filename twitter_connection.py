import tweepy
from tweepy import Stream
from tweepy.streaming import Stream
from tweepy import OAuthHandler
import socket
import json

consumer_key='AovCHQiJok70cBUW3hfwceLvc'
consumer_secret='LP4P8AYADnpzjvPzCq7pWHMU9m1dw7JwsdtjzLcXqNAMlIqxZu'
access_token ='1265609384421457920-OkYPP56kOE3k4gcPn7XGzw4ffrpkrj'
access_secret='TsHAhpzMvxnTtCSOTIyPQ55gxnAYztwgWu7nd0jnAZTxn'

class TweetsListener(Stream):
  # les objets tweet entend des tweets
  def __init__(self, csocket):
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      print("new message")
      # si les tweets dépassent 14
      if "extended_tweet" in msg:
        # ajouter dans la fin de chaque tweet "t_end" 
        self.client_socket\
            .send(str(msg['extended_tweet']['full_text']+"t_end")\
            .encode('utf-8'))         
        print(msg['extended_tweet']['full_text'])
      else:
        # ajouter dans la fin de chaque tweet "t_end" 
        self.client_socket\
            .send(str(msg['text']+"t_end")\
            .encode('utf-8'))
        print(msg['text'])
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True

def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  # authentification est basée sur les tokens
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  # envoyer les données en utilisant Stream API
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track = keyword, languages=["en"])

if __name__ == "__main__":
    # Server (machine local) crée une socket
    s = socket.socket()
    host = "127.0.0.1"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # Server (machine local) entend une connection
    s.listen(4)
    print('socket is listening')
    # retourner la socket et l'adresse à l'autre partie de la connection (partie client) 
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # remplacer dans keyword les tweets à selectionner en fonction du keyword
    sendData(c_socket, keyword = ['worldcup2022'])