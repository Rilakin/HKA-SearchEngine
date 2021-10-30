# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from elasticsearch import Elasticsearch, client
from elasticsearch_dsl import Search, connections, A, Q
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

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
    return response


def search_description(game_id):
    query = Search(index='issa1011_steam_games_description') \
        .using(client_elastic) \
        .query("match", steam_appid=game_id)
    response = query.execute()
    return response


def append_media(search_result):
    result_list = []
    for hit in search_result:
        hit_dict = hit.to_dict()
        game_media = search_media(hit.appid)
        game_description = search_description(hit.appid)
        for media_hit in game_media:
            hit_dict["header_image"] = media_hit.header_image
        for des_hit in game_description:
            hit_dict["short_description"] = des_hit.short_description
            hit_dict["detailed_description"] = des_hit.detailed_description
        result_list.append(hit_dict)
    return result_list


def add_filter(query, genres, categories, platforms, paging):
    # apply filters
    if genres:
        genres_list = genres.split(",")
        for genre in genres_list:
            query = query.filter("match", genres=genre)
    if categories:
        categories_list = categories.split(",")
        for category in categories_list:
            query = query.filter("match", categories=category)
    if platforms:
        platforms_list = platforms.split(",")
        for platform in platforms_list:
            query = query.filter("match", platforms=platform)

    # apply paging
    if paging:
        paging_list = paging.split(",")
        query = query[int(paging_list[0]):int(paging_list[1])]
    else:
        query = query[0:10]
    return query


def search_games(name, genres, categories, platforms, paging):
    # start building search query with name
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", name={'query': name, 'fuzziness': 'AUTO'}))
    query = add_filter(query, genres, categories, platforms, paging)
    response = query.execute()
    return append_media(response)


def search_developers(name, genres, categories, platforms, paging):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", developer={'query': name, 'fuzziness': 'AUTO'}))
    query = add_filter(query, genres, categories, platforms, paging)
    response = query.execute()
    return append_media(response)


def search_publishers(name, genres, categories, platforms, paging):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", publisher={'query': name, 'fuzziness': 'AUTO'}))
    query = add_filter(query, genres, categories, platforms, paging)
    response = query.execute()
    return append_media(response)


def get_metadata():
    query = Search(index='issa1011_steam_games').using(client_elastic)
    query.aggs.bucket('genres', 'terms', field='genres')
    query.aggs.bucket('categories', 'terms', field='categories')
    query.aggs.bucket('platforms', 'terms', field='platforms')
    response = query.execute()

    return response


# parameter example:
# "http://127.0.0.1:5001/games/Counter-Strike?genres=Action&categories=Multi-player&platforms=windows&paging=0,10"
# note that "" is important for windows curl only
@app.get("/games/<game_name>")
def list_games(game_name):
    genres_filter = request.args.get("genres")
    categories_filter = request.args.get("categories")
    platforms_filter = request.args.get("platforms")
    paging = request.args.get("paging")
    result = search_games(game_name, genres_filter, categories_filter, platforms_filter, paging)
    result = jsonify(result)
    result.headers.add("Access-Control-Allow-Origin", "*")
    return result


@app.get("/publisher/<publisher_name>")
def list_publisher(publisher_name):
    genres_filter = request.args.get("genres")
    categories_filter = request.args.get("categories")
    platforms_filter = request.args.get("platforms")
    paging = request.args.get("paging")
    result = search_publishers(publisher_name, genres_filter, categories_filter, platforms_filter, paging)
    result = jsonify(result)
    result.headers.add("Access-Control-Allow-Origin", "*")
    return result


@app.get("/developer/<developer_name>")
def list_developer(developer_name):
    genres_filter = request.args.get("genres")
    categories_filter = request.args.get("categories")
    platforms_filter = request.args.get("platforms")
    paging = request.args.get("paging")
    result = jsonify(search_developers(developer_name, genres_filter, categories_filter, platforms_filter, paging))
    result.headers.add("Access-Control-Allow-Origin", "*")
    return result


@app.get("/genres")
def list_genres():
    result = jsonify(get_metadata().to_dict())
    result.headers.add("Access-Control-Allow-Origin", "*")
    return result


if __name__ == '__main__':
    # search_games("Counter-Strike", "Action")
    # search_developers("Valve", "")
    # get_metadata()
    app.run(host="0.0.0.0", port="5000")
