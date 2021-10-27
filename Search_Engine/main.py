# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from elasticsearch import Elasticsearch, client
from elasticsearch_dsl import Search, connections
from flask import Flask, request, jsonify

app = Flask(__name__)
client_elastic = Elasticsearch([{"host": "node-1.hska.io", "port": 9200},
                                {"host": "node-2.hska.io", "port": 9200}])
if client_elastic.ping():
    print('Connected')
else:
    print('Connection Failed')


def search_media(game_id):
    query = Search(index='issa1011_steam_games_media') \
        .using(client_elastic) \
        .query("match", steam_appid=game_id)
    response = query.execute()
    #print(response.to_dict())
    #result = response.to_dict()
    return response

def search_games(name, genre):
    result_list = []
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query("match", name=name) \
        .filter("match", genres=genre)
    #query.filter('terms', tags=genre)
    response = query.execute()

    for hit in response:
        hit_dict = hit.to_dict()
        game_media = search_media(hit.appid)
        for media_hit in game_media:
            hit_dict["header_image"] = media_hit.header_image
        #hit_dict["header_image"] = game_media["hits"]["_source"]["header_image"]
        #print("Found the game: " + hit.name)
        print(hit_dict)
        result_list.append(hit_dict)

    return response
    # for tag in response.aggregations.per_tag.buckets:
    #    print(tag.key, tag.max_lines.value)


def search_developers(name, genre):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query("match", developer=name) \
        .filter("match", genres=genre)

    response = query.execute()

    for hit in response:
        print("Found the game: " + hit.name)

    return response
    # for tag in response.aggregations.per_tag.buckets:
    #    print(tag.key, tag.max_lines.value)


def search_publishers(name, genre):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query("match", publisher=name) \
        .filter("match", genres=genre)
    response = query.execute()

    for hit in response:
        print("Found the game: " + hit.name)

    return response
    # for tag in response.aggregations.per_tag.buckets:
    #    print(tag.key, tag.max_lines.value)


def get_genres():
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query("match", genres="*")
    response = query.execute()
    response.aggs.bucket('genres', 'terms', field='genres', size=0)

    print(response.aggregations.genres.doc_count)
    print(response.hits.total)
    print(response.aggregations.genres.buckets)

    for item in response.aggregations.genres.buckets:
        print(item)
    return response


@app.get("/games")
def list_games():
    return jsonify(search_games("Counter-Strike", "Action").to_dict())


@app.get("/publisher")
def list_publisher():
    return jsonify(search_publishers("Counter-Strike", "Action").to_dict())


@app.get("/developer")
def list_developer():
    return jsonify(search_developers("Counter-Strike", "Action").to_dict())


@app.get("/genres")
def list_genres():
    return jsonify(get_genres().to_dict())


if __name__ == '__main__':
    #search_games("Counter-Strike", "Action")
    # get_genres()
    app.run(host="127.0.0.1", port="5001")
