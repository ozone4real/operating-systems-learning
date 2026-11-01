FROM gcc
WORKDIR /playground
COPY . /playground
RUN apt-get update -qq && \
  apt-get install -y build-essential \
  clang
CMD [ "/bin/bash" ]