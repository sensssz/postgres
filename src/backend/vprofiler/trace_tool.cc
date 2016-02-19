#include "vprofiler/trace_tool.h"
#include <pthread.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>

using std::ifstream;
using std::ofstream;
using std::getline;
using std::ofstream;
using std::vector;
using std::endl;
using std::string;
using std::to_string;
using std::set;

#define TARGET_PATH_COUNT 2
#define NUMBER_OF_FUNCTIONS 0
#define LATENCY

ulint transaction_id = 0;

class TraceTool {
private:
    static TraceTool *instance;
    /*!< Start time of the current transaction. */
    vector<vector<int> > function_times;
    /*!< Stores the running time of the child functions
                                                 and also transaction latency (the last one). */
    vector<ulint> transaction_start_times;
    /*!< Stores the start time of transactions. */

    TraceTool();

    TraceTool(TraceTool const &) { };
public:
    static timespec global_last_query;
    static __thread timespec trans_start;
    static __thread ulint current_transaction_id;
    /*!< Each thread can execute only one transaction at
                                                          a time. This is the ID of the current transactions. */

    static __thread int path_count;
    /*!< Number of node in the function call path. Used for
                                            tracing running time of functions. */

    static __thread bool is_commit;
    /*!< True if the current transactions commits. */
    static __thread bool commit_successful; /*!< True if the current transaction successfully commits. */
    static bool should_shutdown;
    static pthread_t back_thread;
    static ofstream log_file;

    int id;

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
__thread ulint TraceTool::current_transaction_id = 0;

timespec TraceTool::global_last_query;

ofstream TraceTool::log_file;

__thread int TraceTool::path_count = 0;
__thread bool TraceTool::is_commit = false;
__thread bool TraceTool::commit_successful = true;
__thread timespec TraceTool::trans_start;

bool TraceTool::should_shutdown = false;
pthread_t TraceTool::back_thread;

/* Define MONITOR if needs to trace running time of functions. */
#ifdef MONITOR
static __thread timespec function_start;
static __thread timespec function_end;
static __thread timespec call_start;
static __thread timespec call_end;
#endif

void set_id(int id) {
    TraceTool::get_instance()->id = id;
    if (!TraceTool::log_file.is_open()) {
        TraceTool::log_file.open("logs/log_file_" + to_string(id));
    }
}

int get_thread_id() {
    return TraceTool::get_instance()->id;
}

pthread_t get_thread() {
    return TraceTool::back_thread;
}

void set_should_shutdown(int shutdown) {
    TraceTool::should_shutdown = shutdown;
}

void log_command(const char *command) {
    TraceTool::get_instance()->log_file << "[Thread " << pthread_self() << "]: " << command << endl;
}

void QUERY_START() {
    TraceTool::get_instance()->global_last_query = TraceTool::get_time();
}

void TRX_START() {
#ifdef LATENCY
    TraceTool::get_instance()->start_trx();
#endif
}

void TRX_END() {
#ifdef LATENCY
    TraceTool::get_instance()->end_trx();
#endif
}

void COMMIT(int successful) {
#ifdef LATENCY
    TraceTool::get_instance()->is_commit = true;
    TraceTool::get_instance()->commit_successful = successful;
#endif
}

void PATH_INC() {
#ifdef LATENCY
    TraceTool::get_instance()->path_count++;
//    TraceTool::get_instance()->log_file << pthread_self() << " increments path_count to " << TraceTool::get_instance()->path_count << endl;
#endif
}

void PATH_DEC() {
#ifdef LATENCY
    TraceTool::get_instance()->path_count--;
//    TraceTool::get_instance()->log_file << pthread_self() << " decrements path_count to " << TraceTool::get_instance()->path_count << endl;
#endif
}

void PATH_SET(int path_count) {
#ifdef LATENCY
    TraceTool::get_instance()->path_count = path_count;
#endif
}

int PATH_GET() {
#ifdef LATENCY
    return TraceTool::get_instance()->path_count;
#endif
}

void TRACE_FUNCTION_START() {
//    TraceTool::get_instance()->log_file << "Function starts" << endl;
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
//    TraceTool::get_instance()->log_file << "Function ends" << endl;
#endif
}

int TRACE_START() {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &call_start);
    }
#endif
    return 0;
}

int TRACE_END(int index) {
#ifdef MONITOR
    if (TraceTool::should_monitor()) {
        clock_gettime(CLOCK_REALTIME, &call_end);
        long duration = TraceTool::difftime(call_start, call_end);
        TraceTool::get_instance()->add_record(index, duration);
    }
#endif
    return 0;
}

timespec get_trx_start() {
    return TraceTool::get_instance()->trans_start;
}

/********************************************************************//**
Get the current TraceTool instance. */
TraceTool *TraceTool::get_instance() {
    if (instance == NULL) {
        instance = new TraceTool;
#ifdef LATENCY
        /* Create a background thread for dumping function running time
           and latency data. */
        pthread_create(&back_thread, NULL, check_write_log, NULL);
#endif
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
}

bool TraceTool::should_monitor() {
//    if (path_count != TARGET_PATH_COUNT) {
//        log_file << path_count << endl;
//    }
    return path_count == TARGET_PATH_COUNT;
}

void *TraceTool::check_write_log(void *arg) {
    /* Runs in an infinite loop and for every 5 seconds,
       check if there's any query comes in. If not, then
       dump data to log files. */
    while (true) {
        sleep(5);
//        log_file << "Checking," << global_last_query.tv_sec << ":" << global_last_query.tv_nsec << "," << transaction_id << endl;
        timespec now = get_time();
        if (now.tv_sec - global_last_query.tv_sec >= 10 && transaction_id > 0) {
            /* Create a new TraceTool instance. */
            TraceTool *old_instance = instance;
            instance = new TraceTool;
            instance->id = old_instance->id;

            /* Reset the global transaction ID. */
            transaction_id = 0;

            /* Dump data in the old instance to log files and
               reclaim memory. */
            old_instance->write_log();
            delete old_instance;
        }

        if (now.tv_sec - global_last_query.tv_sec >= 5 && should_shutdown) {
            break;
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
    commit_successful = true;
    /* Use a write lock here because we are appending content to the vector. */
    current_transaction_id = transaction_id++;
    transaction_start_times[current_transaction_id] = now_micro();
    for (vector<vector<int> >::iterator iterator = function_times.begin();
         iterator != function_times.end();
         ++iterator) {
        iterator->push_back(0);
    }
    transaction_start_times.push_back(0);
    clock_gettime(CLOCK_REALTIME, &global_last_query);
    trans_start = get_time();
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
    function_times.back()[current_transaction_id] = (int) latency;
    if (!commit_successful) {
        transaction_start_times[current_transaction_id] = 0;
    }
    is_commit = false;
#endif
}

void TraceTool::add_record(int function_index, long duration) {
    if (current_transaction_id > transaction_id) {
        current_transaction_id = 0;
    }
    function_times[function_index][current_transaction_id] += duration;
}

void TraceTool::write_latency(string dir) {
    log_file << "Thread is " << pthread_self() << endl;
    ofstream tpcc_log;
    tpcc_log.open(dir + "tpcc_" + to_string(id));

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
    tpcc_log.close();
}

void TraceTool::write_log() {
//    log_file << "Write log on instance " << instance << ", id is " << id << endl;
    if (id > 0) {
        write_latency("latency/");
    }
}
