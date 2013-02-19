#include "picojson.h"
#include <curl/curl.h>

using namespace std;
using namespace picojson;

namespace Pusher {
    const char* GCM_PUSH_URL = "https://android.googleapis.com/gcm/send";
    
    /* struct containing data of a thread */
    struct Tdata {
        CURLSH *share;
        char *url;
    };
    struct userdata {
        char *text;
        int counter;
    };
    /* lock callback */
    void lock(CURL *handle, curl_lock_data data, curl_lock_access access,
              void *useptr )
    {
        const char *what;
        struct userdata *user = (struct userdata *)useptr;
        (void)handle;
        (void)access;
        switch ( data ) {
            case CURL_LOCK_DATA_SHARE:
                what = "share";
                break;
            case CURL_LOCK_DATA_DNS:
                what = "dns";
                break;
            case CURL_LOCK_DATA_COOKIE:
                what = "cookie";
                break;
            default:
                fprintf(stderr, "lock: no such data: %d\n", (int)data);
                return;
        }
        printf("lock: %-6s <%s>: %d\n", what, user->text, user->counter);
        user->counter++;
    }
    /* unlock callback */
    void unlock(CURL *handle, curl_lock_data data, void *useptr )
    {
        const char *what;
        struct userdata *user = (struct userdata *)useptr;
        (void)handle;
        switch ( data ) {
            case CURL_LOCK_DATA_SHARE:
                what = "share";
                break;
            case CURL_LOCK_DATA_DNS:
                what = "dns";
                break; 
            case CURL_LOCK_DATA_COOKIE: 
                what = "cookie"; 
                break; 
            default: 
                fprintf(stderr, "unlock: no such data: %d\n", (int)data); 
                return; 
        } 
        printf("unlock: %-6s <%s>: %d\n", what, user->text, user->counter); 
        user->counter++; 
    } 

    static unsigned long gcm_writer(char *data, size_t size, size_t nmemb, std::string *buffer_in)
    {
        // Is there anything in the buffer?
        if(buffer_in != NULL) {
            // Append the data to the buffer
            buffer_in->append(data, size * nmemb);

            // How much did we write?
            return size * nmemb;
        }

        return 0;
    }

    static string gcm_req(string api_key, string post_data)
    {
        CURL *curl;
        CURLcode res;
        struct curl_slist *headers = NULL; // init to NULL is important
        headers = curl_slist_append(headers, ("Authorization: key=" + api_key).c_str());
        headers = curl_slist_append(headers, "Content-Type: application/json");

        // Create our curl handle
        curl = curl_easy_init();

        char errorBuffer[CURL_ERROR_SIZE];

        // Write all expected data in here
        string response;

        if (curl) {
            // Now set up all of the curl options
            curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
            curl_easy_setopt(curl, CURLOPT_URL, GCM_PUSH_URL);
            curl_easy_setopt(curl, CURLOPT_HEADER, false);
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
            
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, gcm_writer);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            
            curl_share_setopt(curl, CURLSHOPT_LOCKFUNC, lock);
            curl_share_setopt(curl, CURLSHOPT_UNLOCKFUNC, unlock);
            
            curl_easy_setopt(curl, CURLOPT_NOSIGNAL, true);
            curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, true); //mimic real world use
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, false);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, false);

            curl_easy_setopt(curl, CURLOPT_POST, true);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());

            // Attempt to retrieve the remote page
            res = curl_easy_perform(curl);

            // Always cleanup
            curl_easy_cleanup(curl);
        }

        if(res != CURLE_OK) {
            cerr << "CURL Error: " << curl_easy_strerror(res) << endl;

            return string();
        }
        
        delete headers;

        return response;
    }

    bool gcm_send(string api_key, vector<string> devices, string data) {
        // Prepare request
        if(devices.size() == 0) {
            return false;
        }
        
        array devices_arr;

        for(int i = 0; i < devices.size(); i++) {
            devices_arr.push_back(* new value(devices[i]));
        }

        object data_obj;
        data_obj["data"] = * new value(data);

        object req_json;
        req_json["registration_ids"] = * new value(devices_arr);
        req_json["data"] = * new value(data_obj);
        value req(req_json);
        
        const char* response;
        
        try {
            response = gcm_req(api_key, req.serialize()).c_str();
        } catch(exception& e) {
            cerr << "Request error: " << e.what() << endl;
        } catch (...) {
            cout << "Request error" << endl;
        }
        
        //cout << response << endl;

        return true;

        // Process response
        picojson::value v;
        std::string err;
        picojson::parse(v, response, response + strlen(response), &err);

        if (!err.empty()) {
            cerr << "JSON Error: " << err << endl;
            cerr << response << endl;

            return false;
        }

        object json = v.get<object>();
        cout << v << endl;

        return true;
    }
}