#!/bin/bash

mv Dockerfile ../
cd ..
docker build -t new-ch-schema .
docker run -d -it --name doc1 new-ch-schema bash
docker exec -it new-ch-schema bash

