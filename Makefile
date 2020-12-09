# 
# Makefile 
#
# Assignment 4 Makefile
#
# University of Toronto
# 2020

PROG=server
SOURCE=$(wildcard *.rs)
MAIN=main.rs

all: $(SOURCE)
	rustc -A unused_variables -A dead_code -o $(PROG) $(MAIN)
	
.PHONY: clean	
clean:
	rm -f $(PROG) tester* *.log

