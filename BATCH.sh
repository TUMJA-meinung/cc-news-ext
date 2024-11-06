#!/bin/sh
CONTAINER="/glob/g01-cache/container/nvpytorch24.05-py3.simg"
singularity exec -H $PWD:/home --nv "${CONTAINER}" /bin/bash -c "cd /home && ./RUN.sh"