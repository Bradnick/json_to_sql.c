/*
   json_to_sql.c Erik Sundlo
   --------------------------
   Generates SQL-insert statements from a JSON-file. Will only insert the columns
   present so should solve the problem when JSON contains varying columns per JSON-object

   The database-table must be created with the same column-names as the column-names
   derived from the JSON-object. I guess the db-columns should just be created as 
   varchar(500) or something.
   
   Should work for all databases, if Oracle or Postgres just substitute the 'GO' with ';'.

   Download jsmn.h from https://github.com/zserge/jsmn  
   
   Compile (i.e.gcc): gcc json_to_sql.c -o json_to_sql
*/
#include "jsmn.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#define MAX_LENGTH 100000
#define SMALL_BUF 1024
#define MAX_KEYS 2048

int giRowCount = 0;

struct _pair
{
  char key[SMALL_BUF + 1];
  char val[SMALL_BUF + 1];
};
struct _pair strKeyPair[MAX_KEYS];

static inline void *realloc_it(void *ptrmem, size_t size)
{
  void *p = realloc(ptrmem, size);
  if (!p)
  {
    free(ptrmem);
    fprintf(stderr, "realloc(): errno=%d\n", errno);
  }
  return p;
}

/*
  Just skip initial underscore and then hyphens and dollar from column-name
*/
void vNiceify(char *psString)
{
  char sBuf[SMALL_BUF];
  char *psTrav = sBuf;
  char *psHead = psString;

  /*Move past first ' and '_' */
  while (*psString == '_' || *psString == '\'') psString++;
  while (*psString != '\0')
  {
    if (*psString != '\'' && *psString != '$') *(psTrav) ++ = toupper(*psString);
    psString++;
  }
  *psTrav = '\0';
  psString = psHead;
  strcpy(psString, sBuf);
  return;
}

/*
  Remove a given char from the string
*/
void vRemovechar(char *psString, char cChar)
{
  char sBuf[SMALL_BUF];
  char *psTrav = sBuf;
  char *psHead = psString;

  while (*psString != '\0')
  {
    if (*psString != cChar) *(psTrav) ++ = *psString;
    psString++;
  }
  *psTrav = '\0';
  psString = psHead;
  strcpy(psString, sBuf);
  return;
}

/*
  'dump' is derived from https://github.com/zserge/jsmn/blob/master/example/jsondump.c
*/
static int dump(const char *js, jsmntok_t *t, size_t count, int indent, char *psResult)
{
  int i, j, k;
  char *psHead = psResult;
  char sEntry[SMALL_BUF];
  if (count == 0)
  {
    return 0;
  }
  if (t->type == JSMN_PRIMITIVE)
  {
    sprintf(sEntry, "%.*s", t->end - t->start, js + t->start);
    strcat(psResult, sEntry);
    return 1;
  }
  else if (t->type == JSMN_STRING)
  {
    sprintf(sEntry, "'%.*s'", t->end - t->start, js + t->start);
    strcat(psResult, sEntry);
    return 1;
  }
  else if (t->type == JSMN_OBJECT)
  {
    j = 0;
    for (i = 0; i < t->size; i++)
    {
      j += dump(js, t + 1 + j, count - j, indent + 1, psResult);
      strcat(strKeyPair[giRowCount].key, psResult);
      *psHead = '\0';

      j += dump(js, t + 1 + j, count - j, indent + 1, psResult);

      if (*psResult != '\0')
      {
        strcpy(strKeyPair[giRowCount].val, psResult);
        giRowCount++;
      }
      *psResult = '\0';
    }
    return j + 1;
  }
  else if (t->type == JSMN_ARRAY)
  {
    j = 0;
    for (i = 0; i < t->size; i++)
    {
      j += dump(js, t + 1 + j, count - j, indent + 1, psResult);
    }
    return j + 1;
  }
  return 0;
}

/*
   Read JSON from file, then process each line and convert into a insert-statement and write to file
*/     
main(int argc, char *argv[])
{
  int r;
  int eof_expected = 0;
  char *js = NULL;
  size_t jslen = 0;
  FILE * fpIn;
  FILE * fpOut;
  char sFileNameIn[500];
  char sFileNameOut[500];
  char *ptrBuf = NULL, *ptrLine = NULL, *ptrLineBufStart = NULL;
  int iLineLength = 0, iReadLength = 0, iCharCount = 0;
  int i = 0;
  int rc = 0;
  char JSON_STRING[MAX_LENGTH];
  char CONV_STRING[MAX_LENGTH];
  char sSystemCall[MAX_LENGTH];
  char sTableName[SMALL_BUF];
  char *psTableName = sTableName;
  char *psConvString = CONV_STRING;
  jsmn_parser p;
  jsmntok_t * tok;
  size_t tokcount = 2;

  /*Allocate some tokens as a start */
  tok = malloc(sizeof(*tok) *tokcount);
  if (tok == NULL)
  {
    fprintf(stderr, "malloc(): errno=%d\n", errno);
    return 3;
  }

  if (argc != 3)
  {
    printf("Use: json_to_sql <filename-in> <filename-out>\n");
    return (1);
  }

  strcpy(sFileNameIn, argv[1]);
  strcpy(sFileNameOut, argv[2]);

  if ((fpIn = fopen(sFileNameIn, "r")) == NULL)
  {
    printf("Cannot open file %s.\n", sFileNameIn);
    return (1);
  }

  if ((fpOut = fopen(sFileNameOut, "w")) == NULL)
  {
    printf("Cannot write to file %s.\n", sFileNameOut);
    return (1);
  }

  ptrLineBufStart = JSON_STRING;
  ptrLine = ptrLineBufStart;
  for (i = 0; i < MAX_LENGTH; i++) *(ptrLine) ++ = '\0';
  while (fgets(ptrLineBufStart, MAX_LENGTH, fpIn) != NULL)
  {
    iCharCount = 0;
    iReadLength = strlen(ptrLineBufStart);
    ptrLine = ptrLineBufStart;
    while (*ptrLine != '\n' && iCharCount < iReadLength)
    {
      ptrLine++;
      iCharCount++;
    }
    *ptrLine = '\0';
    ptrLine = ptrLineBufStart;

    r = sizeof(JSON_STRING);
    js = realloc_it(js, jslen + r + 1);
    if (js == NULL)
    {
      return 3;
    }
    strncpy(js + jslen, JSON_STRING, r);
    strcpy(js, JSON_STRING);
    jslen = jslen + r;

    again:

      jsmn_init(&p);

    /* Init struct */
    for (i = 0; i < MAX_KEYS; i++)
    {
      strcpy(strKeyPair[i].key, "");
      strcpy(strKeyPair[i].val, "");
    }

    giRowCount = 0;

    r = jsmn_parse(&p, js, jslen, tok, tokcount);
    if (r < 0)
    {
      if (r == JSMN_ERROR_NOMEM)
      {
        tokcount = tokcount * 2;
        tok = realloc_it(tok, sizeof(*tok) *tokcount);
        if (tok == NULL)
        {
          return 3;
        }
        goto again;
      }
    }
    else
    {
      dump(js, tok, p.toknext, 0, psConvString);
      eof_expected = 1;
    }
    strcpy(sTableName, basename(sFileNameIn));
    psTableName = sTableName;
    while (*psTableName != '.' && *psTableName != '\0') psTableName++;
    *psTableName = '\0';

    for (i = 0; i < giRowCount; i++)
      vNiceify((char*) &strKeyPair[i].key);
    fprintf(fpOut, "insert into %s (\n", sTableName);
    for (i = 0; i < giRowCount; i++)
      if (i > 0) fprintf(fpOut, ", %s\n", strKeyPair[i].key);
      else fprintf(fpOut, "%s\n", strKeyPair[i].key);
    fprintf(fpOut, ")\nvalues (\n", sTableName);
    for (i = 0; i < giRowCount; i++)
    {
      vRemovechar((char*) &strKeyPair[i].val, '\'');
      if (i > 0) fprintf(fpOut, ", '%s'\n", strKeyPair[i].val);
      else fprintf(fpOut, "'%s'\n", strKeyPair[i].val);
    }
    fprintf(fpOut, ")\nGO\n");
  }

  fflush(fpOut);
  fclose(fpOut);
  free(ptrBuf);
  free(tok);
  fclose(fpIn);

  return EXIT_SUCCESS;
}
