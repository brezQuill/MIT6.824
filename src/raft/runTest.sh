#!/bin/bash


echo -e "\n\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\n\n" >> logs.txt
go test -run TestSnapshotInstallUnCrash2D -race >> logs.txt