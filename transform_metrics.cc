#include <uwsgi.h>
#include <string>
#include "json.hpp"
using json = nlohmann::json;

/**
 * Transforms invalid [1] metric keys (e.g. worker.0.core.0.requests) into
 * JSON pointer notation [2] (e.g. "/workers/0/cores/0/requests"). This is
 * then used by nlohmann::json::patch [3] to set the value in the appropriate
 * place in the stats JSON object.
 *
 * The full stats JSON object contains structures that conveniently map onto
 * the metric keys. For instance, using the example above
 * (worker.1.core.0.requests), we find in the full stats object:
 *
 *     {
 *         "workers": [{
 *             "cores": [{ ... }]
 *         }]
 *     }
 *
 * For metric key components that precede an array index, these must be
 * pluralized (worker, core, and socket become workers, cores, and sockets,
 * respectively).
 *
 * Also note that, in the above example of worker.1.core, the worker id is
 * one-indexed, not zero-indexed. This is because the overall stats apply to
 * the master worker. Thus worker.0.avg_response_time actually corresponds
 * to the JSON path /avg_response_time.
 *
 * [1] In mongo-c-driver >= 1.6.0, documents with keys containing '.' are not
 *     permitted, since the resulting document is almost impossible to query:
 *     '.' is used in queries to indicate object nesting, and there is no way
 *     to escape the character.
 * [2] See https://tools.ietf.org/html/rfc6901
 * [3] See https://github.com/nlohmann/json#json-pointer-and-json-patch
 */
static std::string metrics_key_to_json_pointer_path(std::string key) {
    std::string path = "/";
    path.reserve(key.length() + 2);

    std::string::size_type max = key.length() - 1;
    std::string::size_type last = 0;
    std::string::size_type pos, next;

    std::string component;

    while (std::string::npos != (pos = key.find(".", last))) {
        component = key.substr(last, pos - last);

        bool is_array_key = (pos < max && std::isdigit(key.at(pos + 1)));

        if (is_array_key) {
            component += "s";
            if (component == "workers") {
                // worker index fix
                if (std::string::npos != (next = key.find(".", pos + 1))) {
                    int worker_num = stoi(key.substr(pos + 1, next), nullptr, 10);
                    // If worker == 0, don't append workers/0/ (since the stat
                    // applies to the master worker), otherwise subtract 1 from
                    // the worker id to make it zero-indexed
                    if (worker_num == 0) {
                        // append nothing, continue past the worker id
                        last = next + 1;
                        continue;
                    } else {
                        component += "/" + std::to_string(worker_num - 1);
                        // Advance 'pos' past the worker id, so we don't
                        // append it twice
                        pos = next;
                    }
                }
            }
        }

        path += component + "/";
        last = pos + 1;
    }
    // Add remaining key after the last "."
    path += key.substr(last);
    return path;
}

void transform_metrics(json &doc) {
    auto metrics = doc["metrics"];

    if (metrics.is_null()) {
        return;
    }

    for (json::iterator it = metrics.begin(); it != metrics.end(); ++it) {
        std::string path = metrics_key_to_json_pointer_path(it.key());
        auto value = it.value()["value"];
        if (value.is_null()) {
            continue;
        }

        try {
            doc[json::json_pointer(path)] = value;
        } catch (json::exception &exc) {
            uwsgi_log("[stats-pusher-mongodb] error setting json val for "
                      "metric %s: %s\n", it.key().c_str(), exc.what());
        }
    }

    doc.erase("metrics");
}
