import os
import tweepy
import pandas as pd
from auth import *

class KeyException(Exception):
    def __init__(self, message='Key Empty'):
        # Call the base class constructor with the parameters it needs
        super(KeyEmptyException, self).__init__(message)

####input your credentials here
consumer_key = CONSUMER_KEY
consumer_secret = CONSUMER_SECRET
access_token = ACCESS_KEY
access_token_secret = ACCESS_SECRET

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

def scrape(hashtag, limit= False):
    #defining dataframe to store tweets
    df = pd.DataFrame(columns=['id','texto'])

    #defining query for scraping tweets
    query_hashtag = hashtag + " -filter:retweets"
    
    #defining the file name for the csv that will store the tweets
    storage_filename = "tweets_" + hashtag.replace("#","") + ".csv"

    #verifying if a file for storage already exists
    if os.path.isfile(storage_filename):
        #if exists, reads the id from the most recent tweet and gathers only tweets that are more recent
        temp_db = pd.read_csv(storage_filename, sep='\t')
        last_tweet = str (temp_db.iloc[-1]['id'])
        del temp_db 

        if(not limit):
            cursor_tweets = tweepy.Cursor(api.search,q=query_hashtag,lang="pt",since_id=last_tweet).items()
        else:
            cursor_tweets = tweepy.Cursor(api.search,q=query_hashtag,lang="pt",since_id=last_tweet).items(limit)
    else:
        #if doesnt exist, gathers any tweets in available range
        if(not limit):
            cursor_tweets = tweepy.Cursor(api.search,q=query_hashtag,lang="pt").items()
        else:
            cursor_tweets = tweepy.Cursor(api.search,q=query_hashtag,lang="pt").items(limit)

    #puts the gathered tweets in a list in order from oldest to newest
    tweets_list   = [tweet for tweet in cursor_tweets]
    tweets_list.reverse()
    
    i = 1
    #showing and storing in a dataframe the gathered tweets
    print(f'Exibindo tweets coletados:')
    for tweet in tweets_list:
        print(f'tweet {i}: {tweet.text}')
        ith_tweet = [tweet.id,tweet.text.encode('utf-8')]
        df.loc[len(df)] = ith_tweet
        i = i + 1
    
    #storing tweets
    if os.path.isfile(storage_filename):
        df.to_csv(storage_filename,mode= 'a',index=False, sep ='\t', header = False)
    else:
        df.to_csv(storage_filename,index=False, sep ='\t')

if __name__ == '__main__':
    print("Bem-vindo ao twitter crawler!")
    print()
    print()
    while True:
        try:
            for value in [consumer_key, consumer_secret, access_token, access_token_secret]:
                if (value == None) or value == '':
                    raise KeyException("(ERRO) Declare no cabecalho do script as chaves de acesso de sua conta")
        
            print("Modo de uso:")
            print("\t-Declare a hashtag ou termo que deseja utilizar para coletar os tweets.")
            print("\t-Se desejar definir um limite maximo de tweets a serem coletados, ponha o numero desejado separado por espaco")
            print("\t-Para sair digite: -E")
            print()
            print('Exemplo de entrada:')
            print('\t-Para o maximo possivel de tweets: "#exemplo"')
            print('\t-Para um limite maximo de 150: "#exemplo 150"')
            print()
            print('>', end="")

            search = input().split()
            if (search == ["-E"]):
                break
            if len(search) == 1:
                scrape(search[0])
                break
            elif len(search) == 2:
                if search[1].isnumeric():
                    scrape( search[0], int(search[1]) )
                    break
                else:
                    print()
                    print("(ERRO) Input invalido. O limite especificado nao eh um valor numerico.")
                    print()
                    
            else:
                print()
                print("(ERRO) Input invalido. Insira a entrada conforme os exemplos e o modo de uso especificado.")
                print()
        except KeyException as e:
            print()
            print(e)
            print()
