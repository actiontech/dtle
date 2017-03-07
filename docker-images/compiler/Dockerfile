From docker-registry:5000/centos:centos7
RUN yum install -y rpm-build ruby-devel git vim wget gcc make
RUN gem install fpm

COPY go1.4.3.linux-amd64.tar.gz /opt/
COPY go1.7.5.src.tar.gz /opt/

RUN mkdir /root/go1.4 \
        && cat /opt/go1.4.3.linux-amd64.tar.gz | tar zxC /root/go1.4 --strip-components 1 \
        && cat /opt/go1.7.5.src.tar.gz | tar xzC /opt \
        && cd /opt/go/src \
        && GO_GCFLAGS=-N ./make.bash \
        && rm -rf /root/go1.4 \
        && mkdir -p /workspace/src/udup

ENV GOPATH /workspace
ENV GOROOT /opt/go
ENV PATH $PATH:$GOROOT/bin:$GOPATH

VOLUME /workspace
COPY * /docker-build/
ENTRYPOINT ["/bin/bash"]
