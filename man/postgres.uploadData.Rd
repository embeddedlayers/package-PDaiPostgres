% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/postgres.R
\name{postgres.uploadData}
\alias{postgres.uploadData}
\title{Upload Data to PostgreSQL with Logging}
\usage{
postgres.uploadData(
  .conn,
  .tablename,
  .data,
  .logtable = "uploadlog",
  preprocess = T
)
}
\arguments{
\item{.conn}{A connection object to the PostgreSQL database.}

\item{.tablename}{Name of the destination table in the PostgreSQL database.}

\item{.data}{A dataframe containing the data to be uploaded.}

\item{.logtable}{Name of the table used to log upload processes (default is "uploadlog").}

\item{preprocess}{Logical indicating whether the data should be preprocessed before upload (default is TRUE).}
}
\value{
A string message indicating the status of the data upload.
}
\description{
This function uploads a dataset to a specified PostgreSQL table and logs the process.
If the destination table does not exist, it is created. The function also accounts for
potential errors during the upload process and logs them.
}
\examples{
# Assuming you have an active connection and dataset to upload:
# result <- postgres.uploadData(.conn, .tablename = "mytable", .data = myData)

}
\seealso{
\code{postgres.update}, \code{postgres.delete} and other utility functions.
}
