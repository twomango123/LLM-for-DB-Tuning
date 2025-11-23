#!/bin/bash

mv Dockerfile ../
cd ..
docker build -t ch-schema .
docker run -it ch-schema bash

