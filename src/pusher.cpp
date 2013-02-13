#include <fstream>
#include <mongo/client/dbclient.h>
#include "zmq.hpp"
#include "picojson.h"
#include "web++.hpp"

#include "gcm.hpp"
//#include "apns.hpp"

using namespace std;
using namespace mongo;
using namespace picojson;

object CONFIG;
std::deque<std::string> LOGS;
DBClientConnection* mongo_conn = new DBClientConnection(true, NULL, 20);
string mongo_host;
string mongo_port;
string mongo_db;
string mongo_collection;

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

/*
void* doWork(void* ctx) {
    DBClientConnection mongo_conn;
    mongo_conn.connect(mongo_host + ":" + mongo_port);
    
    zmq::context_t* context = reinterpret_cast<zmq::context_t*>(ctx);
    zmq::socket_t socket(*context, ZMQ_REP);
    socket.connect("inproc://workers");
    
    zmq_pollitem_t items[1] = { { socket, 0, ZMQ_POLLIN, 0 } };
    
    while(true) {
        stringstream log;
        object response;
        double start;
        
        try {
            if(zmq_poll(items, 1, -1) < 1) {
                cout << "Terminating worker" << endl;
                break;
            }
            
            zmq::message_t request;
            socket.recv(&request);
            
            start = timer();
            
            char* json_str = (char*) request.data();
            
            value push;
            std::string err;
            parse(push, json_str, json_str + strlen(json_str), &err);
            
            if (!err.empty()) {
                throw string("Request JSON Parse Error: " + err + " " + json_str);
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
            response.insert(std::make_pair ("status", * new value(status ? "success" : "error")));
            
            // Log
            time_t now = time(0);
            tm *ltm = std::localtime(&now);
            
            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << (status ? "success" : "error") << " " << request.size() << "b "
            << " \t" << timer(start) << "s";
        } catch(string e) {
            response.insert(std::make_pair ("status", * new value("error")));
            response.insert(std::make_pair ("message", * new value(e)));
            
            // Log
            time_t now = time(0);
            tm *ltm = std::localtime(&now);
            
            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << e
            << " \t" << timer(start) << "s";
        } catch(const zmq::error_t& e) {
            response.insert(std::make_pair ("status", * new value("error")));
            response.insert(std::make_pair ("message", * new value(e.what())));
            
            // Log
            time_t now = time(0);
            tm *ltm = std::localtime(&now);
            
            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << "ZeroMQ Worker: " << e.what()
            << " \t" << timer(start) << "s";
        } catch(const mongo::DBException &e) {
            response.insert(std::make_pair ("status", * new value("error")));
            response.insert(std::make_pair ("message", * new value(e.what())));
            
            // Log
            time_t now = time(0);
            tm *ltm = std::localtime(&now);
            
            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << "MongoDB: " << e.what()
            << " \t" << timer(start) << "s";
        } catch(exception& e) {
            response.insert(std::make_pair ("status", * new value("error")));
            response.insert(std::make_pair ("message", * new value(e.what())));
            
            // Log
            time_t now = time(0);
            tm *ltm = std::localtime(&now);
            
            log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
            << "Fatal error: " << e.what()
            << " \t" << timer(start) << "s";
        }
        
        value resp(response);
        string resp_text = resp.serialize();
        zmq::message_t reply(resp_text.size());
        memcpy (reply.data(), resp_text.c_str(), resp_text.size());
        //socket.send(reply);
        
        cout << log.str() << endl;
        
        LOGS.push_back(log.str());
    }
    
    zmq_close(socket);
    
    return NULL;
}
*/

void* ThreadWorker(void* args) {
    stringstream log;
    object response;
    double start;
        
    try {
        DBClientConnection mongo_conn;
        mongo_conn.connect(mongo_host + ":" + mongo_port);
        
        start = timer();
        
        char* json_str = (char*)args;
        
//        sleep(2);
//        
//        cout << json_str << endl;
//
        
        //cout << getpid() << " " << strlen(json_str) << endl;
        //return NULL;
        
        value push;
        std::string err;
        parse(push, json_str, json_str + strlen(json_str), &err);
        
        if (!err.empty()) {
            throw string("Request JSON Parse Error: " + err + " " + json_str);
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
            
            SaveToMongo(&mongo_conn, mongo_db, mongo_collection, data.str());
            
            if(!Pusher::gcm_send(api_key, devices, data.str())) {
                status = false;
            }
        }
        
        // Response back
        response.insert(std::make_pair ("status", * new value(status ? "success" : "error")));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << "[" << (unsigned long)pthread_self() << "] " << (status ? "success" : "error") << " " << strlen(json_str) << "b "
        << " \t" << timer(start) << "s";
    } catch(string e) {
        response.insert(std::make_pair ("status", * new value("error")));
        response.insert(std::make_pair ("message", * new value(e)));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << e
        << " \t" << timer(start) << "s";
    } catch(const zmq::error_t& e) {
        response.insert(std::make_pair ("status", * new value("error")));
        response.insert(std::make_pair ("message", * new value(e.what())));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << "ZeroMQ Worker: " << e.what()
        << " \t" << timer(start) << "s";
    } catch(const mongo::DBException& e) {
        response.insert(std::make_pair ("status", * new value("error")));
        response.insert(std::make_pair ("message", * new value(e.what())));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << "MongoDB: " << e.what()
        << " \t" << timer(start) << "s";
    } catch(exception& e) {
        response.insert(std::make_pair ("status", * new value("error")));
        response.insert(std::make_pair ("message", * new value(e.what())));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << "Fatal error: " << e.what()
        << " \t" << timer(start) << "s";
    }
    
    //value resp(response);
    //string resp_text = resp.serialize();
    //zmq::message_t reply(resp_text.size());
    //memcpy (reply.data(), resp_text.c_str(), resp_text.size());
    //socket.send(reply);
    
    cout << log.str() << endl;
    
    LOGS.push_back(log.str());
    
    return NULL;
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
    mongo_host = CONFIG["mongodb"].get<object>()["host"].to_str();
    mongo_port = CONFIG["mongodb"].get<object>()["port"].to_str();
    mongo_db = CONFIG["mongodb"].get<object>()["db"].to_str();
    mongo_collection = CONFIG["mongodb"].get<object>()["collection"].to_str();
        
    pthread_t workers;
    zmq::context_t context(1);
    zmq::message_t request;
    zmq::socket_t socket(context, ZMQ_PULL);
    
    // Print 0MQ URI
    cout << uri << endl;
    
    try {
        socket.bind(uri.c_str());
        mongo_conn->connect(mongo_host + ":" + mongo_port);
        
        /////////-----
        //        DBClientConnection mongo_conn;
        //        mongo_conn.connect(mongo_host + ":" + mongo_port);
        
        while(true) {
            if(socket.recv(&request) > 0) { //ZMQ_NOBLOCK
                char* data = (char*) request.data();
                
                void *copy = operator new(request.size());
                memcpy(copy, data, request.size());
                
                pthread_create(&workers, NULL, &ThreadWorker, copy);
            }
            
            usleep(10 * 1000);
        }
    }
    catch(const zmq::error_t& e) {
        cerr << "ZeroMQ: " << e.what() << endl;
    }
    catch(exception& e) {
        cerr << "MainThread Exception: " << e.what() << endl;
    }
    
    zmq_close(socket);
    zmq_term(context);
    pthread_join(workers, NULL);
    
    return NULL;
}
/*
void web_interface(WPP::Request* req, WPP::Response* res) {
    array tmp;
    for (int i = 0; i < LOGS.size(); i++) {
        tmp.push_back(* new value(LOGS[i]));
    }
    value resp(tmp);

    res->type = "application/json";
    res->body << resp.serialize().c_str();
}
*/

int main(int argc, const char* argv[]) {
//    pthread_t main;

//    int rc = pthread_create (&main, NULL, &run, NULL);
//    assert (rc == 0);
    
    run(NULL);

//    try {
//        WPP::Server server;
//        server.get("/", &web_interface);
//        server.start(5000);
//    } catch(WPP::Exception e) {
//        cerr << "WebServer: " << e.what() << endl;
//    }

    return EXIT_SUCCESS;
}
