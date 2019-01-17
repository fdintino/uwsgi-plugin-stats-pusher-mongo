#include <uwsgi.h>
#include <string>
#include <mongoc/mongoc.h>
#include <bson/bson.h>
#include "json.hpp"

using json = nlohmann::json;

extern struct uwsgi_server uwsgi;

json transform_metrics(json doc);

struct uwsgi_mongo_stats {
    char *address;
    int freq;
    char *db_coll;
    char *db;
    char *coll;
    mongoc_uri_t *uri;
    mongoc_client_pool_t *pool;
    struct uwsgi_stats_pusher *pusher;
} u_mongo;

static struct uwsgi_option stats_pusher_mongodb_options[] = {
    {(char *)"mongo-stats", required_argument, 0,
        (char *)"server where stats are pushed",
        uwsgi_opt_set_str, &u_mongo.address, 0},
    {(char *)"mongo-stats-collection", required_argument, 0,
        (char *)"collection where stats are pushed (default uwsgi.stats)",
        uwsgi_opt_set_str, &u_mongo.db_coll, 0},
    {(char *)"mongo-stats-freq", required_argument, 0,
        (char *)"set mongo stats push frequency in seconds (default 60)",
        uwsgi_opt_set_int, &u_mongo.freq, 0},
    {0, 0, 0, 0, 0, 0, 0},
};

static int stats_pusher_mongodb_init(void) {
    mongoc_init();
    return 1;
}

static void stats_pusher_mongodb_atexit() {
    if (u_mongo.pool) {
        mongoc_client_pool_destroy(u_mongo.pool);
    }
    if (u_mongo.uri) {
        mongoc_uri_destroy(u_mongo.uri);
    }
    mongoc_cleanup();
}

static void stats_pusher_mongodb_post_init() {
    if (!u_mongo.address) return;
    if (!u_mongo.db_coll) u_mongo.db_coll = (char *)"uwsgi.stats";    
    if (!u_mongo.freq) u_mongo.freq = 60;
    u_mongo.db = uwsgi_str(u_mongo.db_coll);
    u_mongo.coll = strchr(u_mongo.db, '.');
    if (!u_mongo.coll) {
      uwsgi_log("[stats-pusher-mongodb] invalid mongo collection (%s), "
                "must be in the form db.collection\n", u_mongo.db_coll);
      exit(1);
    }
    u_mongo.coll[0] = 0;
    u_mongo.coll++;

    char *uri_string = uwsgi_concat2((char *)"mongodb://", u_mongo.address);
    bson_error_t error;

    if (!(u_mongo.uri = mongoc_uri_new_with_error(uri_string, &error))) {
        uwsgi_log("[stats-pusher-mongodb] failed to parse URI %s: %s\n",
                  u_mongo.address, error.message);
        exit(1);
    }
    u_mongo.pool = mongoc_client_pool_new(u_mongo.uri);
    mongoc_client_pool_set_error_api(u_mongo.pool, 2);

    uwsgi_log("[stats-pusher-mongodb] plugin started, mongodb://%s/%s.%s, %is freq\n",
        u_mongo.address, u_mongo.db, u_mongo.coll, u_mongo.freq);

    struct uwsgi_stats_pusher_instance *uspi = uwsgi_stats_pusher_add(
        u_mongo.pusher, NULL);
    uspi->freq = u_mongo.freq;
    uspi->configured = 1;
}

static void stats_pusher_mongodb_push(struct uwsgi_stats_pusher_instance *uspi,
                                      time_t now, char *json_str, size_t json_len) {
    bson_error_t error;
    mongoc_collection_t *coll;
    mongoc_client_t *client;
    bson_t *bson;
    bson_oid_t oid;
    json doc, transformed;
    std::string str;

    if (!u_mongo.pool) return;
    if (uwsgi.mywid > 0) {
        uwsgi_log("[stats-pusher-mongodb] skipping stats; not master but %is", uwsgi.mywid);
    }

    uint64_t start_push = uwsgi_micros();

    try {
        doc = json::parse(std::string(json_str, json_len));
    } catch (json::exception &e) {
        uwsgi_log("[stats-pusher-mongodb] ERROR(JSON): %s\n", e.what());
        return;
    }
    doc["id"] = std::string(uwsgi_str(uwsgi.sockets->name));

    doc["hostname"] = std::string(uwsgi.hostname, uwsgi.hostname_len);
    if (uwsgi.procname_master) {
        doc["procname"] = uwsgi.procname_master;
    } else if (uwsgi.procname) {
        doc["procname"] = uwsgi.procname;
    }

    transformed = transform_metrics(doc);

    str = transformed.dump().c_str();

    client = mongoc_client_pool_pop(u_mongo.pool);
    coll = mongoc_client_get_collection(client, u_mongo.db, u_mongo.coll);

    if (!(bson = bson_new_from_json((const uint8_t *)str.c_str(), -1, &error))) {
        uwsgi_log("[stats-pusher-mongodb] BSON ERROR(%s/%s): %s\n%s\n",
            u_mongo.address, u_mongo.db_coll, error.message);
        goto done;
    }

    bson_oid_init(&oid, NULL);
    BSON_APPEND_OID(bson, "_id", &oid);

    if (!mongoc_collection_insert_one(coll, bson, NULL, NULL, &error)) {
       uwsgi_log("[stats-pusher-mongodb] MONGO ERROR(%s/%s): %s\n",
           u_mongo.address, u_mongo.db_coll, error.message);
    }

done:
    uint64_t end_push = uwsgi_micros();

    if (bson) bson_destroy(bson);
    if (client) mongoc_client_pool_push(u_mongo.pool, client);
    if (coll) mongoc_collection_destroy(coll);

    uwsgi_log("[stats-pusher-mongodb] finished in %s msec\n",
        uwsgi_64bit2str((end_push - start_push) / 1000));
}

static void stats_pusher_mongodb_on_load(void) {
    u_mongo.pusher = uwsgi_register_stats_pusher(
        (char *)"mongodb", stats_pusher_mongodb_push);
}

extern "C" struct uwsgi_plugin stats_pusher_mongodb_plugin = {
    .name = "stats_pusher_mongodb",
    .alias = NULL,
    .modifier1 = NULL,
    .data = NULL,
    .on_load = stats_pusher_mongodb_on_load,
    .init = stats_pusher_mongodb_init,
    .post_init = stats_pusher_mongodb_post_init,
    .post_fork = NULL,
    .options = stats_pusher_mongodb_options,
    .enable_threads = NULL,
    .init_thread = NULL,
    .request = NULL,
    .after_request = NULL,
    .preinit_apps = NULL,
    .init_apps = NULL,
    .postinit_apps = NULL,
    .fixup = NULL,
    .master_fixup = NULL,
    .master_cycle = NULL,
    .mount_app = NULL,
    .manage_udp = NULL,
    .suspend = NULL,
    .resume = NULL,
    .harakiri = NULL,
    .hijack_worker = NULL,
    .spooler_init = NULL,
    .atexit = stats_pusher_mongodb_atexit
};
