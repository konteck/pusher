//
//  queue++.h
//  Pusher
//
//  Created by Alex Movsisyan.
//  Copyright (c) 2013 Alex Movsisyan. All rights reserved.
//

#include <queue>

using namespace std;

namespace QPP {
    pthread_mutex_t lock;
    
    struct Job {
        void* (*callback)(void*);
        void* args;
    };
    
    class Queue {
    public:
        Queue(int workers_count);
        ~Queue();
        void add_job(void* (*callback)(void*));
        void add_job(void* (*callback)(void*),void*);
        void start();
        void start_nonblocking();
    private:
        static void* run_loop(void*);
        int workers_count;
        deque<Job> jobs_queue;
    };
    
    void* Queue::run_loop(void* context){
        Queue* cls = (Queue*)context;
                 
        int active_threads = 0;
        
        pthread_t workers[cls->workers_count];
                
        while(true) { // !cls->jobs_queue.empty() || active_threads > 0
            active_threads = 0;
            
            try {
                for(int i = 0; i < cls->workers_count; i++) {
                    if(!workers[i] || pthread_kill(workers[i], 0) != 0) {
                        pthread_mutex_lock(&lock);
                        if(cls->jobs_queue.size() > 0) {
                            Job job = cls->jobs_queue.front();
                        
                            pthread_create (&workers[i], NULL, job.callback, job.args);
                            cls->jobs_queue.pop_front();
                            
                            //cout << (workers[0] == NULL) << endl;
                        }
                        pthread_mutex_unlock(&lock);
                    } else { // still running
                        //active_threads++;
                    
                        //cout << "Active thread" << endl;
                    }
                }
            } catch(exception& e) {
                pthread_mutex_unlock(&lock);
                
                cout << "Queue Error: " << e.what() << endl;
            }
            
            usleep(1000);
        }
        
        return NULL;
    }
    
    Queue::Queue(int workers_count){
        this->workers_count = workers_count;
        
        if (pthread_mutex_init(&lock, NULL) != 0) {
            cout << "Mutex init failed" << endl;
        }
    }
    
    Queue::~Queue(){
        pthread_mutex_destroy(&lock);
    }
    
    void Queue::add_job(void* (*callback)(void*)){
        this->add_job(callback, NULL);
    }
    
    void Queue::add_job(void* (*callback)(void*), void* args){
        Job j = {
            callback,
            args
        };
        pthread_mutex_lock(&lock);
        this->jobs_queue.push_back(j);
        pthread_mutex_unlock(&lock);
    }
    
    void Queue::start(){
        this->run_loop(this);
    }
    
    void Queue::start_nonblocking(){
        pthread_t main;
        assert (pthread_create (&main, NULL, this->run_loop, this) == 0);
    }
}
