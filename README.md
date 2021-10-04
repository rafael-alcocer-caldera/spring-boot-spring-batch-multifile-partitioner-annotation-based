# SPRING BOOT SPRING BATCH PARTITIONING EXAMPLE

## Synopsis

The project is a Spring Boot Application with Spring Batch Partitioning annotation based to process multiple files.

## Motivation

I wanted to do a Spring Boot Application that uses Spring Batch Partitioning.

## How to run the job

http://localhost:8080/getThings

## How this works

You can put any number of files within the directory "\data\inbound", and will be read by this application.
The only restriction is that it needs to have 3 columns separated by a delimiter, for instance "|".

You can change the commit interval.
Currently in the application.properties commit interval has a value of 3.
The value of the commit interval means the number of every row of each file that will be read.
Currently we have 3 files, animals.txt, devices.txt, and numbers.txt, and every 3 rows of each file will be read before write.

The items size in the writer will be 3.

## License

All work is under Apache 2.0 license