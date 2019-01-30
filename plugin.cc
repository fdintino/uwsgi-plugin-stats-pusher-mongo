#include <uwsgi.h>
#include <string>
#include <stdlib.h>
#include <cstdint>
#include <mongoc/mongoc.h>
#include <bson/bson.h>
#include "json.hpp"

using json = nlohmann::json;

extern struct uwsgi_server uwsgi;

void transform_metrics(json &doc);

#define LG0(err)      uwsgi_log("[stats-pusher-mongodb] " err "\n")
#define LOG(err, ...) uwsgi_log("[stats-pusher-mongodb] " err "\n", __VA_ARGS__)
#define DBG(err, ...) if (u_mongo.verbose) uwsgi_log("[stats-pusher-mongodb] " err "\n", __VA_ARGS__)


struct uwsgi_mongo_keyval {
    json::json_pointer key;
    std::string val_str;
    long long val_int;
    bool is_int;
};

struct uwsgi_mongo_stats {
    char *address;
    int freq;
    char *db_coll;
    char *db;
    char *coll;
    bool verbose;
    mongoc_uri_t *uri;
    mongoc_client_pool_t *pool;
    struct uwsgi_string_list *custom_kvals_str;
    struct uwsgi_string_list *custom_kvals_int;
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
    {(char *)"mongo-stats-kv", required_argument, 0,
        (char *)"add a custom key/value to the stats json",
        uwsgi_opt_add_string_list, &u_mongo.custom_kvals_str, 0},
    {(char *)"mongo-stats-kv-int", required_argument, 0,
        (char *)"add a custom int key/value to the stats json",
        uwsgi_opt_add_string_list, &u_mongo.custom_kvals_int, 0},
    {(char *)"mongo-stats-verbose", no_argument, 0,
        (char *)"enable verbose log messages",
        uwsgi_opt_true, &u_mongo.verbose, 0},
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

static void stats_pusher_mongodb_register_keyval(uwsgi_string_list *usl, bool is_int) {
    std::string str, key, val;
    std::size_t pos;
    auto kv = new uwsgi_mongo_keyval;

    kv->is_int = is_int;

    str = std::string(usl->value);

    pos = str.find('=');
    if (pos == std::string::npos) {
        LG0("invalid keyval; missing '='");
        return;
    }

    key = str.substr(0, pos);
    val = str.substr(pos + 1, std::string::npos);

    try {
        kv->key = json::json_pointer(key);
    } catch (json::exception &exc) {
        LOG("invalid keyval json pointer in '%s': %s",
            kv->val_str.c_str(), exc.what());
        return;
    }
    if (!kv->is_int) {
        kv->val_str = val;
    } else {
        try {
            kv->val_int = std::stoll(val, nullptr);
        } catch (std::invalid_argument &ia) {
            LOG("int conversion error of keyval in '%s'=%s: %s",
                key.c_str(), val.c_str(), ia.what());
            return;
        } catch (std::out_of_range &orr) {
            LOG("out-of-range error for keyval '%s'=%s to long long: %s",
                key.c_str(), val.c_str(), orr.what());
            return;
        }
    }
    usl->custom_ptr = (void *)kv;
    DBG("added custom keyval: %s=%s", key.c_str(), val.c_str());
}

static void stats_pusher_mongodb_set_doc_val(json &doc, uwsgi_string_list *usl) {
    if (usl->custom_ptr == NULL) {
        return;
    }
    struct uwsgi_mongo_keyval *kv = (struct uwsgi_mongo_keyval *)usl->custom_ptr;
    try {
        if (kv->is_int) {
            doc[kv->key] = kv->val_int;
        } else {
            doc[kv->key] = kv->val_str;
        }
    } catch (json::exception &exc) {
        LOG("error setting custom keyval: %s: %s",
            kv->key.to_string().c_str(), exc.what());
    }
}

static void stats_pusher_mongodb_update_doc(json &doc) {
    struct uwsgi_string_list *usl;
    uwsgi_foreach(usl, u_mongo.custom_kvals_str) {
        stats_pusher_mongodb_set_doc_val(doc, usl);
    }
    uwsgi_foreach(usl, u_mongo.custom_kvals_int) {
        stats_pusher_mongodb_set_doc_val(doc, usl);
    }
}

static void stats_pusher_mongodb_post_init() {
    if (!u_mongo.address) return;
    if (!u_mongo.db_coll) u_mongo.db_coll = (char *)"uwsgi.stats";    
    if (!u_mongo.freq) u_mongo.freq = 60;
    u_mongo.db = uwsgi_str(u_mongo.db_coll);
    u_mongo.coll = strchr(u_mongo.db, '.');
    if (!u_mongo.coll) {
        LOG("invalid mongo collection (%s), must be in the form db.collection",
            u_mongo.db_coll);
        exit(1);
    }
    u_mongo.coll[0] = 0;
    u_mongo.coll++;

    char *uri_string = uwsgi_concat2((char *)"mongodb://", u_mongo.address);
    bson_error_t error;

    if (!(u_mongo.uri = mongoc_uri_new_with_error(uri_string, &error))) {
        LOG("failed to parse URI %s: %s", u_mongo.address, error.message);
        exit(1);
    }
    u_mongo.pool = mongoc_client_pool_new(u_mongo.uri);
    mongoc_client_pool_set_error_api(u_mongo.pool, 2);

    struct uwsgi_stats_pusher_instance *uspi = uwsgi_stats_pusher_add(
        u_mongo.pusher, NULL);
    uspi->freq = u_mongo.freq;

    struct uwsgi_string_list *usl;
    uwsgi_foreach(usl, u_mongo.custom_kvals_str) {
        stats_pusher_mongodb_register_keyval(usl, false);
    }
    uwsgi_foreach(usl, u_mongo.custom_kvals_int) {
        stats_pusher_mongodb_register_keyval(usl, true);
    }

    uspi->configured = 1;

    LOG("plugin started, mongodb://%s/%s.%s, %is freq",
        u_mongo.address, u_mongo.db, u_mongo.coll, u_mongo.freq);
}

static void stats_pusher_mongodb_push(struct uwsgi_stats_pusher_instance *uspi,
                                      time_t now, char *json_str, size_t json_len) {
    bson_error_t error;
    mongoc_collection_t *coll;
    mongoc_client_t *client;
    bson_t *bson;
    bson_oid_t oid;
    json doc;

    if (!u_mongo.pool) return;
    if (uwsgi.mywid > 0) {
        LOG("skipping stats; not master but %i", uwsgi.mywid);
    }

    uint64_t start_push = uwsgi_micros();

    try {
        doc = json::parse(std::string(json_str, json_len));
    } catch (json::exception &e) {
        LOG("ERROR(JSON): %s", e.what());
        return;
    }
    if (uwsgi.procname_master) {
        doc["procname"] = uwsgi.procname_master;
    } else if (uwsgi.procname) {
        doc["procname"] = uwsgi.procname;
    }

    stats_pusher_mongodb_update_doc(doc);
    transform_metrics(doc);

    client = mongoc_client_pool_pop(u_mongo.pool);
    coll = mongoc_client_get_collection(client, u_mongo.db, u_mongo.coll);

    std::string str = doc.dump();
    if (!(bson = bson_new_from_json((const uint8_t *)str.c_str(), -1, &error))) {
        LOG("BSON ERROR(%s/%s): %s", u_mongo.address, u_mongo.db_coll,
            error.message);
        goto done;
    }

    bson_oid_init(&oid, NULL);
    BSON_APPEND_OID(bson, "_id", &oid);

    if (!mongoc_collection_insert_one(coll, bson, NULL, NULL, &error)) {
       LOG("MONGO ERROR(%s/%s): %s", u_mongo.address, u_mongo.db_coll,
           error.message);
    }

done:
    uint64_t end_push = uwsgi_micros();

    if (bson) bson_destroy(bson);
    if (client) mongoc_client_pool_push(u_mongo.pool, client);
    if (coll) mongoc_collection_destroy(coll);

    DBG("finished in %s msec", uwsgi_64bit2str((end_push - start_push) / 1000));
}

static void stats_pusher_mongodb_on_load(void) {
    u_mongo.pusher = uwsgi_register_stats_pusher(
        (char *)"mongodb", stats_pusher_mongodb_push);
}

extern "C" struct uwsgi_plugin stats_pusher_mongodb_plugin = {
    .name = "stats_pusher_mongodb",
    .alias = NULL,
    .modifier1 = 0,
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
