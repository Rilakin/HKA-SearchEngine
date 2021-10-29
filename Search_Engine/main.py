# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from elasticsearch import Elasticsearch, client
from elasticsearch_dsl import Search, connections, A, Q
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
    # print(response.to_dict())
    # result = response.to_dict()
    return response


def append_media(search_result):
    result_list = []
    for hit in search_result:
        hit_dict = hit.to_dict()
        game_media = search_media(hit.appid)
        for media_hit in game_media:
            hit_dict["header_image"] = media_hit.header_image
        # hit_dict["header_image"] = game_media["hits"]["_source"]["header_image"]
        # print("Found the game: " + hit.name)
        # print(hit_dict)
        result_list.append(hit_dict)
    return result_list


def search_games(name, genres, categories, platforms, paging):
    #start building search query with name
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", name={'query': name, 'fuzziness': 'AUTO'}))

    #apply filters
    if genres:
        query = query.filter("terms", genres=genres.split(","))
    if categories:
        query = query.filter("terms", categories=categories.split(","))
    if platforms:
        query = query.filter("terms", platforms=platforms.split(","))

    #apply paging
    if paging:
        query = query[int(paging[0]):int(paging[1])]
    else:
        query = query[0:10]

    response = query.execute()

    return append_media(response)


def search_developers(name, genres, categories, platforms, paging):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", developer={'query': name, 'fuzziness': 'AUTO'}))

    # apply filters
    if genres:
        query = query.filter("terms", genres=genres.split(","))
    if categories:
        query = query.filter("terms", categories=categories.split(","))
    if platforms:
        query = query.filter("terms", platforms=platforms.split(","))

    # apply paging
    if paging:
        query = query[int(paging[0]):int(paging[1])]
    else:
        query = query[0:10]


    response = query.execute()

    return append_media(response)


def search_publishers(name, genres, categories, platforms, paging):
    query = Search(index='issa1011_steam_games') \
        .using(client_elastic) \
        .query(Q("match", publisher={'query': name, 'fuzziness': 'AUTO'}))

    # apply filters
    if genres:
        query = query.filter("terms", genres=genres.split(","))
    if categories:
        query = query.filter("terms", categories=categories.split(","))
    if platforms:
        query = query.filter("terms", platforms=platforms.split(","))

    # apply paging
    if paging:
        query = query[int(paging[0]):int(paging[1])]
    else:
        query = query[0:10]

    response = query.execute()

    return append_media(response)


def get_metadata():
    query = Search(index='issa1011_steam_games').using(client_elastic)
    query.aggs.bucket('genres', 'terms', field='genres')
    query.aggs.bucket('categories', 'terms', field='categories')
    query.aggs.bucket('platforms', 'terms', field='platforms')
    response = query.execute()

    print(response.aggregations.genres.buckets)
    print(response.aggregations.categories.buckets)
    print(response.aggregations.platforms.buckets)

    return response


# parameter example:
# "http://127.0.0.1:5001/games/Counter-Strike?genres=Action,Strategy&categories=Singleplayer&platforms=Windows&paging=10,20"
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
    return jsonify(get_metadata().to_dict())


if __name__ == '__main__':
    # search_games("Counter-Strike", "Action")
    # search_developers("Valve", "")
    #get_metadata()
    app.run(host="127.0.0.1", port="5001")
