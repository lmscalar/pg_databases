//
//  main.cpp
//  pg_connect
//
//  Created by Luis Molina on 2019/03/25/.
//  Copyright © 2019 Luis Molina. All rights reserved.
//

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <libpq-fe.h>
#include <string>

using namespace std;


static void
CloseConn(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

// Establish connection to database
PGconn *ConnectDB()
{
    PGconn *dbconn = nullptr;
    // string query_str = "SELECT * FROM futures where contractquotename='CLJ19'";
    
    const char *connection_str = "dbname=securities_database user=postgres password=inster137$ hostaddr=127.0.0.1 port=5432";
    dbconn = PQconnectdb(connection_str);
    
    if(PQstatus(dbconn) != CONNECTION_OK){
        cout << "Unable to connect to database!" << endl;
        CloseConn(dbconn);
    }
    cout << "Connected to database!" << endl;
    
    return dbconn;
    
}

// Create employee table
void CreateEmployeeTable(PGconn *conn)
{
    // Execute with sql statement
    PGresult *res = PQexec(conn, "CREATE TABLE employee (first_name char(30), last_name char(30))");
    
    if(PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("Create employee table failed");
        PQclear(res);
        CloseConn(conn);
    }
    printf("Create employee table - OK\n");
    // Clear result
    PQclear(res);
}

// Append SQL statement and insert record into employee table
void InsertEmpoyeeRec(PGconn *conn, const char * fname, const char * lname)
{
    // Append the SQL statement
    string sSQL;
    sSQL.append("INSERT INTO employee VALUES ('");
    sSQL.append(fname);
    sSQL.append("', '");
    sSQL.append(lname);
    sSQL.append("')");
    
    //Excecute with sql statement
    PGresult *res = PQexec(conn, sSQL.c_str());
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("Insert employee record failed");
        PQclear(res);
        CloseConn(conn);
    }
    printf("Insert employee record - OK\n");
    
    // Clear result
    PQclear(res);
}

// Fetch employee record and display it on screen
void FetchEmployeeRec(PGconn *conn)
{
    // This will hold the number of fields in employee table
    int nFields;
    
    // Start a transaction block
    PGresult *res = PQexec(conn, "BEGIN");
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("BEGIN command failed!");
        PQclear(res);
        CloseConn(conn);
    }
    // Clear result
    PQclear(res);
    
    // Fetch rows from employee table
    // res = PQexec(conn, "DECLARE emprec CURSOR FOR SELECT * from employee");
    res = PQexec(conn, "DECLARE emprec CURSOR FOR SELECT tradedate, open_, high, low, settle FROM futures WHERE contractquotename='CLK19' AND tradedate >= '2019-01-01'");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("DECLARE CURSOR failed!");
        PQclear(res);
        CloseConn(conn);
    }
    
    // Clear result
    PQclear(res);
    
    res = PQexec(conn, "FETCH ALL in emprec");
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        printf("FETCH ALL failed!");
        PQclear(res);
        CloseConn(conn);
    }
    
    // Get the field name
    nFields = PQnfields(res);
    
    // Prepare the header with employee table field name
    // printf("\nFetch employee record: ");
    printf("\n********************************************************************************\n");
    for (int i = 0; i < nFields; i++)
        printf("%-15s", PQfname(res, i));
    printf("\n*********************************************************************************\n");
    
    // Nest, print out the employee record for each row
    for (int i = 0; i < PQntuples(res); i++)
    {
        for (int j = 0; j < nFields; j++)
            printf("%-15s", PQgetvalue(res, i, j));
        printf("\n");
    }
    PQclear(res);
    
    // Close the emprec
    res = PQexec(conn, "CLOSE emprec");
    PQclear(res);
    
    // End the transaction
    res = PQexec(conn, "END");
    
    // Clear result
    PQclear(res);
    
}

// Erase all record in employee table
void RemoveEmployeeRec(PGconn *conn)
{
    // Execute with sql statement
    PGresult *res = PQexec(conn, "DELETE FROM employee");
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("Delete employees record failed.");
        PQclear(res);
        CloseConn(conn);
    }
    
    printf("\nDeleted employee records - OK\n");
    
    // Clear result
    PQclear(res);
}


// Drop employee table from the database
void DropEmployeeTable(PGconn *conn)
{
    // Execute with sql statement
    PGresult *res = PQexec(conn, "DROP TABLE employee");
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        printf("Drop employee table failed.\n");
        PQclear(res);
        CloseConn(conn);
    }
    printf("Drop employee table - OK\n");
    
    // Clear result
    PQclear(res);
    
}

int main(int argc, const char * argv[]) {
    
    PGconn *dbconn = nullptr;
    dbconn = ConnectDB();
    // CreateEmployeeTable(dbconn);
    //InsertEmpoyeeRec(dbconn, "Mario", "Brother");
    //InsertEmpoyeeRec(dbconn, "John",  "Doe");
    FetchEmployeeRec(dbconn);
    
    //printf("\nPress ENTER to remove all records & table...\n");
    //getchar();
    
    //RemoveEmployeeRec(dbconn);
    //DropEmployeeTable(dbconn);
    
    CloseConn(dbconn);
    return 0;
}
