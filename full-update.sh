#!/bin/bash

flask roistat update-db -df=2020-01-01 -dt=2023-08-04
flask roistat leads-processing -df=2020-01-01 -dt=2023-08-04

