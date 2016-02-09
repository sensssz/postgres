#include "vprofiler/trace_tool.h"
#include <pthread.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <vector>

using std::ifstream;
using std::ofstream;
using std::getline;
using std::ofstream;
using std::vector;
using std::endl;
using std::string;

#define TARGET_PATH_COUNT 1
#define NUMBER_OF_FUNCTIONS 33
#define LATENCY
#define MONITOR

ulint transaction_id = 0;

class TraceTool {
private:
    static TraceTool *instance;
    /*!< Instance for the Singleton pattern. */
    static pthread_mutex_t instance_mutex;
    /*!< Mutex for protecting instance. */

    static timespec global_last_query;
    /*!< Time when MySQL receives the most recent query. */
    static pthread_mutex_t last_query_mutex;
    static __thread timespec trans_start;
    /*!< Start time of the current transaction. */
    vector<vector<int> > function_times;
    /*!< Stores the running time of the child functions
                                                 and also transaction latency (the last one). */
    vector<ulint> transaction_start_times;
    /*!< Stores the start time of transactions. */

    ofstream log_file;

    TraceTool();

    TraceTool(TraceTool const &) { };
public:
    static pthread_rwlock_t data_lock;
    /*!< A read-write lock for protecting function_times. */
    static __thread ulint current_transaction_id;
    /*!< Each thread can execute only one transaction at
                                                          a time. This is the ID of the current transactions. */

    static __thread int path_count;
    /*!< Number of node in the function call path. Used for
                                            tracing running time of functions. */

    static __thread bool is_commit;
    /*!< True if the current transactions commits. */
    static __thread bool commit_successful; /*!< True if the current transaction successfully commits. */


    /********************************************************************//**
    The Singleton pattern. Used for getting the instance of this class. */
    static TraceTool *get_instance();

    /********************************************************************//**
    Check if we should trace the running time of function calls. */
    static bool should_monitor();

    /********************************************************************//**
    Calculate time interval in nanoseconds. */
    static long difftime(timespec start, timespec end);

    /********************************************************************//**
    Periodically checks if any query comes in in the last 5 second.
    If no then dump all logs to disk. */
    static void *check_write_log(void *);

    /********************************************************************//**
    Get the current time in nanosecond. */
    static timespec get_time();
    /********************************************************************//**
    Get the current time in microsecond. */
    static ulint now_micro();

    /********************************************************************//**
    Start a new query. This may also start a new transaction. */
    void start_trx();
    /********************************************************************//**
    End a new query. This may also end the current transaction. */
    void end_trx();
    /********************************************************************//**
    End the current transaction. */
    void end_transaction();
    /********************************************************************//**
    Dump data about function running time and latency to log file. */
    void write_latency(string dir);
    /********************************************************************//**
    Write necessary data to log files. */
    void write_log();

    /********************************************************************//**
    Record running time of a function. */
    void add_record(int function_index, long duration);
};

TraceTool *TraceTool::instance = NULL;
pthread_mutex_t TraceTool::instance_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t TraceTool::data_lock = PTHREAD_RWLOCK_INITIALIZER;
__thread ulint TraceTool::current_transaction_id = 0;

timespec TraceTool::global_last_query;
pthread_mutex_t TraceTool::last_query_mutex = PTHREAD_MUTEX_INITIALIZER;

__thread int TraceTool::path_count = 0;
__thread bool TraceTool::is_commit = false;
__thread bool TraceTool::commit_successful = true;
__thread timespec TraceTool::trans_start;

/* Define MONITOR if needs to trace running time of functions. */
#ifdef MONITOR
static __thread timespec function_start;
static __thread timespec function_end;
static __thread timespec call_start;
static __thread timespec call_end;
#endif

void TRX_START() {
#ifdef MONITOR
    TraceTool::get_instance()->start_trx();
#endif
}

void TRX_END() {
#ifdef MONITOR
    TraceTool::get_instance()->end_trx();
#endif
}

void COMMIT(bool successful) {
#ifdef MONITOR
    TraceTool::get_instance()->is_commit = true;
    TraceTool::get_instance()->commit_successful = successful;
#endif
}

void PATH_INC() {
#ifdef MONITOR
    TraceTool::get_instance()->path_count++;
#endif
}

void PATH_DEC() {
#ifdef MONITOR
    TraceTool::get_instance()->path_count--;
#endif
}

void TRACE_FUNCTION_START() {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &function_start);
    }
#endif
}

void TRACE_FUNCTION_END() {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &function_end);
        long duration = TraceTool::difftime(function_start, function_end);
        TraceTool::get_instance()->add_record(0, duration);
    }
#endif
}

bool TRACE_START() {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &call_start);
    }
#endif
    return false;
}

bool TRACE_END(int index) {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &call_end);
        long duration = TraceTool::difftime(call_start, call_end);
        TraceTool::get_instance()->add_record(index, duration);
    }
#endif
    return false;
}

/********************************************************************//**
Get the current TraceTool instance. */
TraceTool *TraceTool::get_instance() {
    if (instance == NULL) {
        pthread_mutex_lock(&instance_mutex);
        /* Check instance again after entering the critical section
           to prevent double initialization. */
        if (instance == NULL) {
            instance = new TraceTool;
#ifdef LATENCY
            /* Create a background thread for dumping function running time
               and latency data. */
            pthread_t write_thread;
            pthread_create(&write_thread, NULL, check_write_log, NULL);
#endif
        }
        pthread_mutex_unlock(&instance_mutex);
    }
    return instance;
}

TraceTool::TraceTool() : function_times() {
    /* Open the log file in append mode so that it won't be overwritten */
#if defined(MONITOR) || defined(WORK_WAIT)
    const int number_of_functions = NUMBER_OF_FUNCTIONS + 2;
#else
    const int number_of_functions = NUMBER_OF_FUNCTIONS + 1;
#endif
    vector<int> function_time;
    function_time.push_back(0);
    for (int index = 0; index < number_of_functions; index++) {
        function_times.push_back(function_time);
        function_times[index].reserve(500000);
    }
    transaction_start_times.reserve(500000);
    transaction_start_times.push_back(0);

    srand(time(0));
    log_file.open("log_file");
}

bool TraceTool::should_monitor() {
    return path_count == TARGET_PATH_COUNT;
}

void *TraceTool::check_write_log(void *arg) {
    /* Runs in an infinite loop and for every 5 seconds,
       check if there's any query comes in. If not, then
       dump data to log files. */
    while (true) {
        sleep(5);
        instance->log_file << "Checking" << endl;
        timespec now = get_time();
        if (now.tv_sec - global_last_query.tv_sec >= 5 && transaction_id > 0) {
            /* Create a back up of the debug log file in case it's overwritten. */
            std::ifstream src("logs/trace.log", std::ios::binary);
            std::ofstream dst("logs/trace.bak", std::ios::binary);
            dst << src.rdbuf();
            src.close();
            dst.close();

            /* Create a new TraceTool instnance. */
            TraceTool *old_instace = instance;
            instance = new TraceTool;

            /* Reset the global transaction ID. */
            transaction_id = 0;

            /* Dump data in the old instance to log files and
               reclaim memory. */
            old_instace->write_log();
            delete old_instace;
        }
    }
    return NULL;
}

timespec TraceTool::get_time() {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    return now;
}

long TraceTool::difftime(timespec start, timespec end) {
    return (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
}

ulint TraceTool::now_micro() {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    return now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

/********************************************************************//**
Start a new query. This may also start a new transaction. */
void TraceTool::start_trx() {
    is_commit = false;
    /* This happens when a log write happens, which marks the end of a phase. */
    if (current_transaction_id > transaction_id) {
        current_transaction_id = 0;
    }
#ifdef LATENCY
    trans_start = get_time();
    commit_successful = true;
    /* Use a write lock here because we are appending content to the vector. */
    pthread_rwlock_wrlock(&data_lock);
    current_transaction_id = transaction_id++;
    transaction_start_times[current_transaction_id] = now_micro();
    for (vector<vector<int> >::iterator iterator = function_times.begin();
         iterator != function_times.end();
         ++iterator) {
        iterator->push_back(0);
    }
    transaction_start_times.push_back(0);
    pthread_rwlock_unlock(&data_lock);
    pthread_mutex_lock(&last_query_mutex);
    clock_gettime(CLOCK_REALTIME, &global_last_query);
    pthread_mutex_unlock(&last_query_mutex);
#endif
}

void TraceTool::end_trx() {
#ifdef LATENCY
    if (is_commit) {
        end_transaction();
    }
#endif
}

void TraceTool::end_transaction() {
#ifdef LATENCY
    timespec now = get_time();
    long latency = difftime(trans_start, now);
    pthread_rwlock_rdlock(&data_lock);
    function_times.back()[current_transaction_id] = latency;
    if (!commit_successful) {
        transaction_start_times[current_transaction_id] = 0;
    }
    pthread_rwlock_unlock(&data_lock);
#endif
}

void TraceTool::add_record(int function_index, long duration) {
    if (current_transaction_id > transaction_id) {
        current_transaction_id = 0;
    }
    pthread_rwlock_rdlock(&data_lock);
    function_times[function_index][current_transaction_id] += duration;
    pthread_rwlock_unlock(&data_lock);
}

void TraceTool::write_latency(string dir) {
    ofstream tpcc_log;
    tpcc_log.open(dir + "tpcc");

    pthread_rwlock_wrlock(&data_lock);
    for (ulint index = 0; index < transaction_start_times.size(); ++index) {
        ulint start_time = transaction_start_times[index];
        if (start_time > 0) {
            tpcc_log << start_time << endl;
        }
    }

    int function_index = 0;
    for (vector<vector<int> >::iterator iterator = function_times.begin();
         iterator != function_times.end(); ++iterator) {
        ulint number_of_transactions = iterator->size();
        for (ulint index = 0; index < number_of_transactions; ++index) {
            if (transaction_start_times[index] > 0) {
                long latency = (*iterator)[index];
                tpcc_log << function_index << ',' << latency << endl;
            }
        }
        function_index++;
        vector<int>().swap(*iterator);
    }
    vector<vector<int> >().swap(function_times);
    pthread_rwlock_unlock(&data_lock);
    tpcc_log.close();
}

void TraceTool::write_log() {
    write_latency("latency/");
}
