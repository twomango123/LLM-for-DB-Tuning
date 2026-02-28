#!/bin/bash

mv Dockerfile ../
cd ..
docker build -t new-ch-schema .
docker run -d -it --name doc-ch new-ch-schema bash
docker exec -it doc-ch bash