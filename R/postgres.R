#' PostgreSQL Connection Function Using .pgpass File
#'
#' This function establishes a connection to a PostgreSQL database using the parameters
#' specified in the .pgpass file. It allows you to connect without providing a password
#' separately.
#'
#' @return A connection object (`con`) to the PostgreSQL database.
#' @export
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @examples
#' # Connect to a PostgreSQL database using .pgpass file
#' con <- postgres.connect.pgpass()
#'devtools::document()
postgres.connect.pgpass <- function() {
  # Load the .pgpass file
  pgpass_file <- "/home/rstudio/.pgpass"
  pgpass_content <- try(readLines(pgpass_file), silent = TRUE)

  if (inherits(pgpass_content, "try-error")) {
    stop("Failed to read .pgpass file. Make sure it exists and has the correct permissions.")
  }

  # Parse the .pgpass content
  pgpass_fields <- strsplit(pgpass_content, ":")[[1]]

  if (length(pgpass_fields) < 5) {
    stop("Invalid .pgpass file format. It should contain at least 5 fields separated by colons.")
  }

  # Extract connection parameters from .pgpass
  .database <- tolower(pgpass_fields[[1]])
  .port <- as.integer(pgpass_fields[[2]])
  .host <- pgpass_fields[[3]]
  .user <- pgpass_fields[[4]]
  .password <- pgpass_fields[[5]]

  # Build the connection string
  connection_string <- sprintf(
    "dbname=%s host=%s port=%s user=%s",
    .database, .host, .port, .user
  )

  # Connect using RPostgres
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = .database,
    host = .host,
    port = .port,
    user = .user,
    password = .password,
  )

  return(con)
}


#' PostgreSQL Connection Function
#'
#' This function establishes a connection to a PostgreSQL database using the provided
#' parameters. It allows you to set the database name, port, host, user, and password.
#'
#' @param .database Name of the database to connect to (default is NULL). This parameter will be converted to lowercase.
#' @param .port Port number of the PostgreSQL server (default is 5432).
#' @param .host Hostname of the PostgreSQL server (default is 'localhost').
#' @param .user Username to authenticate with (default is 'postgres').
#' @param .password Password for the provided username (default is 'password').
#' @return A connection object (`con`) to the specified PostgreSQL database.
#' @export
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#' @examples
#' # Connect to a local PostgreSQL database
#' con <- postgres.connect(.database = "mydatabase", .user = "myuser", .password = "mypassword")
#'
postgres.connect <-function(
    .database = Sys.getenv("DATABASE_NAME"),
    .port = 5432,
    .host = Sys.getenv("DATABASE_HOST"),
    .user = Sys.getenv("DATABASE_USER"),
    .password = Sys.getenv("DATABASE_PASSWORD")) {

  .database <- tolower(.database)

  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = .host,
    port = .port,
    dbname = .database,
    user = .user,
    password = .password
  )
  return(con)
}


#' Upload Data to PostgreSQL with Logging
#'
#' This function uploads a dataset to a specified PostgreSQL table and logs the process.
#' If the destination table does not exist, it is created. The function also accounts for
#' potential errors during the upload process and logs them.
#'
#' @param .conn A connection object to the PostgreSQL database.
#' @param .tablename Name of the destination table in the PostgreSQL database.
#' @param .data A dataframe containing the data to be uploaded.
#' @param .logtable Name of the table used to log upload processes (default is "uploadlog").
#' @param preprocess Logical indicating whether the data should be preprocessed before upload (default is TRUE).
#' @return A string message indicating the status of the data upload.
#' @export
#' @importFrom uuid UUIDgenerate
#' @importFrom DBI dbListTables dbWriteTable dbExecute
#' @seealso `postgres.update`, `postgres.delete` and other utility functions.
#' @examples
#' # Assuming you have an active connection and dataset to upload:
#' # result <- postgres.uploadData(.conn, .tablename = "mytable", .data = myData)
#'
postgres.uploadData <- function(.conn, .tablename, .data, .logtable = "uploadlog", preprocess = T) {
  .tablename <- tolower(.tablename)
  .logtable <- tolower(.logtable)

  print('--------starting uploaddata--------')

  jobnumber <- uuid::UUIDgenerate(use.time = T)

  tables <- dbListTables(.conn)

  if (!(.logtable %in% tables)) {
    print('creating log table...')
    createLogTableQuery <- paste("CREATE TABLE", .logtable, "(
                                  jobnumber VARCHAR(100),
                                  tablename VARCHAR(255),
                                  uploadtime TIMESTAMP,
                                  rowsuploaded INT,
                                  status VARCHAR(255),
                                  errormessage TEXT,
                                  sysname VARCHAR(255),
                                  releaseinfo VARCHAR(255),
                                  versioninfo VARCHAR(255),
                                  nodename VARCHAR(255),
                                  machine VARCHAR(255),
                                  login VARCHAR(255),
                                  usr VARCHAR(255),
                                  effectiveuser VARCHAR(255));")
    dbExecute(.conn, createLogTableQuery)
  }

  uploadstatus <- ""
  errormsg <- ""

  sysinfo <- Sys.info()
  print('creating log table...')

  logdata <- data.frame(jobnumber = jobnumber,
                        tablename = .tablename,
                        uploadtime = Sys.time(),
                        sysname = sysinfo["sysname"],
                        releaseinfo = sysinfo["release"],
                        versioninfo = sysinfo["version"],
                        nodename = sysinfo["nodename"],
                        machine = sysinfo["machine"],
                        login = sysinfo["login"],
                        usr = sysinfo["user"],
                        effectiveuser = sysinfo["effective.user"],
                        stringsAsFactors = FALSE)
  print('writing to log table...')

  dbWriteTable(.conn, .logtable, logdata, append = TRUE, row.names = FALSE)

  tryCatch(
    expr = {
      if (preprocess) {
        print('preprocessing data...')

        numeric_indices <- sapply(.data, is.numeric)

        # Assuming .data is a data.table
        if (any(numeric_indices)) {
          .data[, numeric_indices] <- lapply(.data[, numeric_indices, drop = FALSE], function(x) {
            ifelse(is.infinite(x) | is.nan(x), NA, x)
          })
        }

        # Detect JSON columns
        json_indices <- unlist(sapply(.data, function(x) class(x) == "json"))

        # Convert JSON columns to character
        if (any(json_indices)) {
          .data[json_indices] <- lapply(.data[json_indices], as.character)
        }

        # Convert column names to lowercase
        names(.data) <- tolower(names(.data))

        print('preprocessing done.')


      }

      sysinfo <- Sys.info()

      .data$jobnumber <- jobnumber
      print(paste('job number:',jobnumber))

      if (!(.tablename %in% tables)) {
        print('creating table...')

        createTableQuery <- paste("CREATE TABLE", .tablename, "(",
                                  paste("",(colnames(.data))," ",sep='',
                                        sapply(sapply(.data, function(x) class(x)[1]), function(x) switch(x,
                                                                                                          "integer" = "INT",
                                                                                                          "numeric" = "DOUBLE PRECISION",
                                                                                                          "character" = "VARCHAR(255)",
                                                                                                          "factor" = "VARCHAR(255)",
                                                                                                          "Date" = "DATE",
                                                                                                          "logical" = "BOOLEAN",
                                                                                                          "POSIXct" = "TIMESTAMP",
                                                                                                          "POSIXt" = "TIMESTAMP",
                                                                                                          "NULL" = "VARCHAR(255)")),
                                        collapse = ", "), ");")
        dbExecute(.conn, createTableQuery)
      }
      print('uploading data...')
      print(.data)

      dbWriteTable(.conn, .tablename, .data, append = T, row.names = FALSE)

      count <- odbc::dbGetQuery(.conn, paste0("select count(*) CNT from ", .tablename, " where jobnumber = '", jobnumber, "'"))

      if (nrow(.data) != as.numeric(count$cnt)) {
        datatable = nrow(.data)
        actual = count$CNT
        stop(paste0('not a full upload: data table contains ', datatable, ' and uploaded ', actual))
      } else {
        postgres.update(.conn, .tablename = .logtable, .predicatelist = list('jobnumber' = jobnumber), .updatelist = list('rowsuploaded' = as.numeric(count$cnt)))
      }

      print('--------completed uploaddata--------')
      return(TRUE)

    },
    error = function(e) {
      print('--------errored uploaddata--------')

      try(postgres.delete(.conn, .tablename, .predicatelist = list('jobnumber' = jobnumber)))
      error <- e

      postgres.update(.conn, .tablename = .logtable, .predicatelist = list('jobnumber' = jobnumber), .updatelist = list('status' = 'failed'))

      print(paste('printing error:',error))
      postgres.update(.conn, .tablename = .logtable, .predicatelist = list('jobnumber' = jobnumber), .updatelist = list('errormessage' = as.character(error)))
      return(e)
    }
  )
  print('--------uploaddata--------')

  return('completed')
}

#' Delete Rows from PostgreSQL Table Based on Conditions
#'
#' This function deletes rows from a specified PostgreSQL table based on conditions provided in a predicate list.
#'
#' @param .conn A connection object to the PostgreSQL database.
#' @param .tablename Name of the table in the PostgreSQL database from which rows will be deleted.
#' @param .predicatelist A named list where names represent the column names and the list elements are the values used in the WHERE clause for deletion.
#' @return A string representation of the WHERE clause used for the deletion.
#' @export
#' @importFrom DBI dbExecute
#' @examples
#' # Assuming you have an active connection:
#' # clause <- postgres.delete(.conn, .tablename = "mytable", .predicatelist = list(column1 = "value1", column2 = "value2"))
#' # print(clause)
#'
postgres.delete <- function(.conn, .tablename, .predicatelist) {

  .tablename <- tolower(.tablename)


  clause <-
    paste(paste(names(.predicatelist), paste0("'", unlist(.predicatelist), "'"), sep = "="), collapse = " and ")
  dbExecute(.conn, paste0("delete from ", .tablename, " where ", clause))
  return(clause)
}

#' Update Rows in a PostgreSQL Table Based on Conditions
#'
#' This function updates rows in a specified PostgreSQL table based on conditions provided in a predicate list
#' and changes specified in an update list.
#'
#' @param .conn A connection object to the PostgreSQL database.
#' @param .tablename Name of the table in the PostgreSQL database where rows will be updated.
#' @param .predicatelist A named list where names represent the column names and the list elements are the values used in the WHERE clause for updating.
#' @param .updatelist A named list where names represent the column names and the list elements are the new values to set.
#' @return A string representation of the SQL UPDATE statement executed.
#' @export
#' @importFrom DBI dbExecute
#' @examples
#' # Assuming you have an active connection:
#' # statement <- postgres.update(.conn, .tablename = "mytable", .predicatelist = list(id = 1), .updatelist = list(name = "new_name"))
#' # print(statement)
#'
postgres.update <- function(.conn, .tablename, .predicatelist, .updatelist) {

  .tablename <- tolower(.tablename)

  clause <-
    paste(paste(names(.predicatelist), paste0("'", unlist(.predicatelist), "'"), sep = "="), collapse = " and ")
  update <-
    paste(paste(names(.updatelist), paste0("'", unlist(.updatelist), "'"), sep = "="), collapse = ", ")
  statement <-
    paste0("update ", .tablename, "  set ", update, "  where ", clause)

  cat("----- UPDATING DATA -----\n")
  cat("Table:", .tablename, "\n")
  cat("Criteria:", clause, "\n")

  dbExecute(.conn, statement)
  return(statement)
}

#' Query Data from a PostgreSQL Table with Optional Parameters
#'
#' This function retrieves data from a specified PostgreSQL table. Users can specify several parameters
#' to refine their queries, such as limiting the number of rows, selecting specific columns, and adding
#' conditions or predicates. There's also an option to randomize the result rows.
#'
#' @param .conn A connection object to the PostgreSQL database.
#' @param .tablename Name of the table in the PostgreSQL database from which data will be retrieved.
#' @param .top An integer specifying the number of rows to retrieve. If NULL (default), all rows are retrieved.
#' @param .columns A vector of column names to retrieve. If NULL (default), all columns are retrieved.
#' @param .predicatelist A named list where names represent column names and the list elements are values used in the WHERE clause for querying.
#' @param .random A logical indicating whether to randomize the order of the result rows. Default is FALSE.
#' @param .print A logical indicating whether to print the SQL query statement. Default is TRUE.
#' @return A data.table containing the query results.
#' @export
#' @importFrom DBI dbGetQuery
#' @importFrom data.table data.table
#' @examples
#' # Assuming you have an active connection:
#' # dt <- postgres.query(.conn, .tablename = "mytable", .top = 10, .columns = c("id", "name"), .predicatelist = list(id = 1))
#' # print(dt)
#'
postgres.query <- function(.conn, .tablename, .top = NULL, .columns = NULL, .predicatelist = NULL, .random = F, .print = T) {

  .tablename <- tolower(.tablename)


  if (!is.null(.predicatelist)) {
    clause <-
      paste0(" Where ", paste(paste(
        names(.predicatelist), paste0("'", unlist(.predicatelist), "'"), sep = "="
      ), collapse = " and "))
  } else {
    clause <- ""
  }

  statement <- paste0("select * from ", .tablename, clause)
  if (.random) {
    statement <- paste0(statement, " ORDER BY RANDOM()")
  }
  if (!is.null(.top)) {
    statement <- paste0(statement, " LIMIT ", .top)
  }
  if (!is.null(.columns)) {
    columns <- paste(.columns, collapse = ",")
    statement <- gsub("\\*", columns, statement)
  }
  if (.print) {
    print(statement)
  }
  return(data.table(odbc::dbGetQuery(.conn, statement)))
}

#' Log API Request in PostgreSQL
#'
#' This function logs API request and response data to a PostgreSQL database.
#'
#' @param .name Name of the API request
#' @param .url URL of the API request
#' @param .body Body of the API request
#' @param .response Response from the API
#' @param .tablename Name of the table to log the data, default is "api_request_logs"
#'
#' @export
#' @importFrom jsonlite toJSON
#' @importFrom DBI dbDisconnect
#' @importFrom RPostgres Postgres
postgres.log.apiRequest <- function(.name, .url, .body, .response,.tablename = "api_request_logs") {
  # Establish Database Connection
  con <- postgres.connect.pgpass()

  # Prepare Data for Logging
  log_entry <- data.frame(
    name = .name,
    url = .url,
    request = jsonlite::toJSON(.body, auto_unbox = TRUE),
    response = jsonlite::toJSON(.response, auto_unbox = TRUE),
    time = Sys.time(),
    stringsAsFactors = FALSE
  )

  # Upload Data to Database
  postgres.uploadData(.conn = con, .tablename = .tablename, .data = log_entry)

  # Disconnect from the Database
  dbDisconnect(con)
}
