#!/bin/bash

mkdir data
wget -P data/ http://projects.csail.mit.edu/dnd/DBLP/dblp.json.gz
gzip -d data/dblp.json.gz