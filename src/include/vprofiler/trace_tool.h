#ifndef MY_TRACE_TOOL_H
#define MY_TRACE_TOOL_H

#ifdef __cplusplus
extern "C" {
#endif

/** This macro is used for tracing the running time of
    a function call which appears inside an if statement*/
#define TRACE_S_E(function_call, index) (TRACE_START()|(function_call)|TRACE_END(index))

typedef unsigned long int ulint;

/** The global transaction id counter */
extern ulint transaction_id;

/********************************************************************//**
To break down the variance of a function, we need to trace the running
time of a function and the functions it calls. */

void QUERY_START();

void QUERY_END(bool commit);

/********************************************************************//**
This function marks the start of a function call */
void TRACE_FUNCTION_START();

/********************************************************************//**
This function marks the end of a function call */
void TRACE_FUNCTION_END();

/********************************************************************//**
This function marks the start of a child function call. */
bool TRACE_START();

/********************************************************************//**
This function marks the end of a child function call. */
bool TRACE_END(
  int index);   /*!< Index of the child function call. */

#ifdef __cplusplus
}
#endif

#endif
