#!/bin/sh

javac -encoding UTF-8 -cp . ticketingsystem/Test.java
java -cp . ticketingsystem/Test 4 0
java -cp . ticketingsystem/Test 8 0
java -cp . ticketingsystem/Test 16 0
java -cp . ticketingsystem/Test 32 0
java -cp . ticketingsystem/Test 64 1
