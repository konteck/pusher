#include <fstream>
#include <mongo/client/dbclient.h>
#include "zmq.hpp"
#include "picojson.h"
#include "web++.hpp"
#include "queue++.h"

#include "gcm.hpp"
//#include "apns.hpp"

using namespace std;
using namespace mongo;
using namespace picojson;

object CONFIG;
std::deque<std::string> LOGS;
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

void* ThreadWorker(void* args) {
    stringstream log;
    object response;
    double start = timer();
        
    try {
        //DBClientConnection mongo_conn;
        //mongo_conn.connect(mongo_host + ":" + mongo_port);
                
        char* json_str = (char*)args;
        
        //sleep(1);
        //cout << json_str << endl;
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
            
            BSONObj p = mongo::fromjson(JSON["gcm"].get<object>()["data"].serialize());
            BSONObjBuilder oB;
            oB.genOID();
            oB.appendElements(p);
            //oB.appendNumber("id", 3333);
            p = oB.obj();
            
            BSONElement oid;
            p.getObjectID(oid);
            
            JSON["gcm"].get<object>()["data"].get<object>().erase("id");
            JSON["gcm"].get<object>()["data"].get<object>().insert(std::make_pair ("id", oid.__oid().toString()));
                        
            scoped_ptr<ScopedDbConnection> conn(ScopedDbConnection::getScopedDbConnection (mongo_host + ":" + mongo_port) );
            conn->get()->insert(mongo_db + "." + mongo_collection, p);
            conn->done();
            
            if(!Pusher::gcm_send(api_key, devices, JSON["gcm"].get<object>()["data"].serialize())) {
                status = false;
            }
            
            //cout << JSON["gcm"].get<object>()["data"].serialize() << endl << p.toString() << endl;
        }
        
        // Response back
        response.insert(std::make_pair ("status", * new value(status ? "success" : "error")));
        
        // Log
        time_t now = time(0);
        tm *ltm = std::localtime(&now);
        
        log << "[" << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec << " " << ltm->tm_mday << "/" << ltm->tm_mon << "] \t"
        << "[" << (unsigned long)pthread_self() << "] " << (status ? "success" : "error") << " " << strlen(json_str) << "b "
        << " \t" << timer(start) << "s";
        
        delete json_str;
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
    } catch (...) {
        cout << "Thread error" << endl;
    }
    
    //value resp(response);
    //string resp_text = resp.serialize();
    //zmq::message_t reply(resp_text.size());
    //memcpy (reply.data(), resp_text.c_str(), resp_text.size());
    //socket.send(reply);
    
    cout << log.str() << endl;
    
    //LOGS.push_back(log.str());
    
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
    int workers_count = std::atoi(CONFIG["zeromq"].get<object>()["workers_count"].to_str().c_str());
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
        //mongo_conn.connect(mongo_host + ":" + mongo_port);
        
        QPP::Queue jobs(workers_count);
        jobs.start_nonblocking();
        
        /* Must initialize libcurl before any threads are started */
        curl_global_init(CURL_GLOBAL_ALL);
        Pusher::init_locks();
        
        while(true) {
            if(socket.recv(&request) > 0) { //ZMQ_NOBLOCK
                void *copy = operator new(request.size());
                memcpy(copy, (char*) request.data(), request.size());
                
                jobs.add_job(&ThreadWorker, copy);
                
                //pthread_create(&workers, NULL, &ThreadWorker, copy);
            }
            
            usleep(1000);
        }
    }
    catch(const zmq::error_t& e) {
        cerr << "ZeroMQ: " << e.what() << endl;
    }
    catch(exception& e) {
        cerr << "MainThread Exception: " << e.what() << endl;
    }
    catch (...) {
        cout << "MainThread Exception" << endl;
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
