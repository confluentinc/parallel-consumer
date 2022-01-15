#!/usr/bin/env bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#

export JAVA_HOME=$(/usr/libexec/java_home -v13)
mvn deploy 
