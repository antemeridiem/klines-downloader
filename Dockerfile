FROM python:3.11

RUN apt-get update 

RUN pip install --upgrade pip wheel
RUN pip install \
    orjson pandas pyarrow requests retry \
    jupyter notebook \
    pyyaml \
    'black[jupyter]'

CMD ["/bin/bash"]