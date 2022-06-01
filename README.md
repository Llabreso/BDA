#!/bin/bash
# Aquest script permet baixar un fitxer i descomprimir-ho
wget $1 -O capitalbike.zip
# Opcionalment es pot pujar el fitxer zip
#hdfs dfs -put capitalbike.zip /user/cloudera/WorkspaceOozie
unzip -c capitalbike.zip > capitalbike.csv
hdfs dfs -put capitalbike.csv /user/cloudera/WorkspaceOozie
