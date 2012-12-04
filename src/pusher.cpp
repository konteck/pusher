#include <fstream>
#include <mongo/client/dbclient.h>
#include "zmq.hpp"
#include "picojson.h"
#include "web++.hpp"

#include "gcm.hpp"

using namespace std;
using namespace mongo;
using namespace picojson;

object CONFIG;
std::deque<std::string> LOGS;

double timer() {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return (double) tv.tv_usec/1000000 + (double) tv.tv_sec;
}

double timer(double start) {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return ((double) tv.tv_usec/1000000 + (double) tv.tv_sec) - start;
}

void SaveToMongo(DBClientConnection* con, string db, string collection, const string data) {
    BSONObj b = mongo::fromjson(data);

    con->insert(db + "." + collection, b);
}

void* doWork(void* ctx) {
    double start;

    string mongo_host = CONFIG["mongodb"].get<object>()["host"].to_str();
    string mongo_port = CONFIG["mongodb"].get<object>()["port"].to_str();
    string mongo_db = CONFIG["mongodb"].get<object>()["db"].to_str();
    string mongo_collection = CONFIG["mongodb"].get<object>()["collection"].to_str();

    DBClientConnection mongo_conn;
    mongo_conn.connect(mongo_host + ":" + mongo_port);

    zmq::context_t* context = reinterpret_cast<zmq::context_t*>(ctx);
    zmq::socket_t socket(*context, ZMQ_REP);
    socket.connect("inproc://workers");

    zmq_pollitem_t items[1] = { { socket, 0, ZMQ_POLLIN, 0 } };

    while(true) {
        try {
            if(zmq_poll(items, 1, -1) < 1) {
                cout << "Terminating worker" << endl;
                break;
            }

            zmq::message_t request;
            socket.recv(&request, ZMQ_DONTWAIT);

            start = timer();

            char* json_str = (char*) request.data();

            value push;
            std::string err;
            parse(push, json_str, json_str + strlen(json_str), &err);

            if (!err.empty()) {
                std::cerr << "Request: " << err << std::endl;

                continue;
            }

            object JSON = push.get<object>();
            bool status = true;

            if(JSON["gcm"].is<object>()) {
                string api_key = CONFIG["gcm"].get<object>()["api_key"].to_str();

                vector<value> pDevices = JSON["gcm"].get<object>()["devices"].get<array>();
                vector<string> devices;

                for(int i = 0; i < pDevices.size(); i++) {
                    devices.push_back(pDevices[i].to_str());
                }

                stringstream data;
                data << JSON["gcm"].get<object>()["data"];

                if(!Pusher::gcm_send(api_key, devices, data.str())) {
                    status = false;
                }

                SaveToMongo(&mongo_conn, mongo_db, mongo_collection, data.str());
            }

            // Response back
            object tmp;
            tmp.insert(std::make_pair ("status", * new value(status ? "success" : "error")));
            value resp(tmp);

            string resp_text = resp.serialize();
            zmq::message_t reply(resp_text.size());
            memcpy (reply.data(), resp_text.c_str(), resp_text.size());
            socket.send(reply);

            // Log
            stringstream log;
            time_t now = time(0);
            tm *ltm = std::localtime(&now);

            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << (status ? "success" : "error") << " " << request.size() << "b "
            << " \t" << timer(start) << "s";

            cout << log.str() << endl;

            LOGS.push_back(log.str());
        } catch(string e) {
            // Image format incorrect
            object tmp;
            tmp.insert(std::make_pair ("status", * new value("error")));
            tmp.insert(std::make_pair ("message", * new value(e)));
            value resp(tmp);

            string resp_text = resp.serialize();
            zmq::message_t reply(resp_text.size());
            memcpy (reply.data(), resp_text.c_str(), resp_text.size());
            socket.send(reply);

            // Log
            stringstream log;
            time_t now = time(0);
            tm *ltm = std::localtime(&now);

            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << e
            << " \t" << timer(start) << "s";

            cerr << log.str() << endl;

            LOGS.push_back(log.str());
        } catch(const zmq::error_t& e) {
            cerr << "ZeroMQ Worker: " << e.what() << endl;
        } catch(const mongo::DBException &e) {
            cerr << "MongoDB: " << e.what() << endl;
        } catch(exception& e) {
            cerr << "Fatal error: " << e.what() << endl;
        }
    }

    zmq_close(socket);
}

void start_queue(string uri, string workers_count) {
    try {
        zmq::context_t context(1);
        zmq::socket_t clients(context, ZMQ_ROUTER);
        clients.bind(uri.c_str());

        zmq::socket_t workers(context, ZMQ_DEALER);
        workers.bind("inproc://workers");

        int WORKERS_COUNT;

        if(workers_count == "auto") {
             WORKERS_COUNT = sysconf(_SC_NPROCESSORS_ONLN); // numCPU
        } else {
             WORKERS_COUNT = atoi(workers_count.c_str());
        }

        pthread_t worker;

        for(int i = 0; i < WORKERS_COUNT; ++i) {
             int rc = pthread_create (&worker, NULL, &doWork, &context);
             assert (rc == 0);
        }

        const int NR_ITEMS = 2;
        zmq_pollitem_t items[NR_ITEMS] =
        {
            { clients, 0, ZMQ_POLLIN, 0 },
            { workers, 0, ZMQ_POLLIN, 0 }
        };

        while(true)
        {
            zmq_poll(items, NR_ITEMS, -1);

            if(items[0].revents & ZMQ_POLLIN) {
                int more; size_t sockOptSize = sizeof(int);

                do {
                    zmq::message_t msg;
                    clients.recv(&msg);
                    clients.getsockopt(ZMQ_RCVMORE, &more, &sockOptSize);

                    workers.send(msg, more ? ZMQ_SNDMORE : ZMQ_DONTWAIT);
                } while(more);
            }

            if(items[1].revents & ZMQ_POLLIN) {
                int more; size_t sockOptSize = sizeof(int);

                do {
                    zmq::message_t msg;
                    workers.recv(&msg);
                    workers.getsockopt(ZMQ_RCVMORE, &more, &sockOptSize);

                    clients.send(msg, more ? ZMQ_SNDMORE : ZMQ_DONTWAIT);
                } while(more);
            }
        }

        zmq_close(clients);
        zmq_close(workers);
        zmq_term(context);
        pthread_join(worker, NULL);
    }
    catch(const zmq::error_t& e) {
       cerr << "ZeroMQ: " << e.what() << endl;
    }
}

void* run(void* arg) {
    // Config
    ifstream infile;
    infile.open ("/etc/pusher.json", ifstream::in);

    if(!infile.is_open()) {
        infile.open ("./pusher.json", ifstream::in);

        if(!infile.is_open()) {
            std::cerr << "CONFIG: Unable to find 'pusher.json' file!" << std::endl;
        }
    }

    value v;
    infile >> v;

    std::string err = get_last_error();

    if (!err.empty()) {
      std::cerr << "pusher.json: " << err << std::endl;
    }

    CONFIG = v.get<object>();

    string uri = CONFIG["zeromq"].get<object>()["uri"].to_str();
    string workers_count = CONFIG["zeromq"].get<object>()["workers_count"].to_str();
    start_queue(uri, workers_count);
}

void web_interface(WPP::Request* req, WPP::Response* res) {
    array tmp;
    for (int i = 0; i < LOGS.size(); i++) {
        tmp.push_back(* new value(LOGS[i]));
    }
    value resp(tmp);

    res->type = "application/json";
    res->body << resp.serialize().c_str();
}

int main(int argc, const char* argv[]) {
    pthread_t main;

    int rc = pthread_create (&main, NULL, &run, NULL);
    assert (rc == 0);

//    run();

    try {
        WPP::Server server;
        server.get("/", &web_interface);
        server.start(5000);
    } catch(WPP::Exception e) {
        cerr << "WebServer: " << e.what() << endl;
    }

    return EXIT_SUCCESS;
}
