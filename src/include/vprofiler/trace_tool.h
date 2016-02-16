#ifndef MY_TRACE_TOOL_H
#define MY_TRACE_TOOL_H

#include <pthread.h>
#include <time.h>

typedef struct timespec timespec;

#ifdef __cplusplus
extern "C" {
#endif

/** This macro is used for tracing the running time of
    a function call which appears inside an if statement*/
#define TRACE_S_E(function_call, index) (TRACE_START()|(function_call)|TRACE_END(index))

typedef unsigned long int ulint;

/** The global transaction id counter */
extern ulint transaction_id;

pthread_t get_thread();

void set_should_shutdown(int shutdown);

void set_id(int id);

int get_thread_id();

void log_command(const char *command);

void QUERY_START();

void TRX_START();

void TRX_END();

void COMMIT(int successful);

void PATH_SET(int path_count);

int PATH_GET();

void PATH_INC();

void PATH_DEC();

timespec get_trx_start();

/********************************************************************//**
This function marks the start of a function call */
void TRACE_FUNCTION_START();

/********************************************************************//**
This function marks the end of a function call */
void TRACE_FUNCTION_END();

/********************************************************************//**
This function marks the start of a child function call. */
int TRACE_START();

/********************************************************************//**
This function marks the end of a child function call. */
int TRACE_END(
  int index);   /*!< Index of the child function call. */

#ifdef __cplusplus
}
#endif

#endif
