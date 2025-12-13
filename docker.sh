#!/bin/bash

mv Dockerfile ../
cd ..
docker build -t new-ch-schema .
docker run -it new-ch-schema bash

