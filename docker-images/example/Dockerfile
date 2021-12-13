From centos:centos7

#
# Set the version, home directory.
#
ENV DTLE_VERSION=9.9.9.9 \
    DTLE_HOME=/dtle

#
# Create a user and home directory for Dtle
#
USER root
RUN groupadd -r dtle -g 1001 && useradd -u 1001 -r -g dtle -m -d $DTLE_HOME -s /sbin/nologin -c "Dtle user" dtle && \
    chmod 755 $DTLE_HOME

#
# Download Dtle and Install
#
RUN yum install -y iproute && \
    yum clean all && \
    curl -fSL -O -u ftpuser:ftpuser ftp://10.186.18.90/actiontech-dtle-ce/qa/$DTLE_VERSION/dtle-ce-$DTLE_VERSION.x86_64.rpm && \
    rpm -ivh --prefix=/dtle dtle-ce-$DTLE_VERSION.x86_64.rpm && \
    rm dtle-ce-$DTLE_VERSION.x86_64.rpm

#
# Change ownership and switch use\
#
RUN chown -R dtle $DTLE_HOME && \
    chgrp -R dtle $DTLE_HOME
USER dtle

# Set the working directory to the Dtle home directory
WORKDIR $DTLE_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 4646
EXPOSE 8190
VOLUME ["/dtle"]

COPY ./nomad-docker.hcl /dtle/etc/dtle/nomad.hcl

CMD ["/dtle/usr/bin/nomad", "agent", "-config", "/dtle/etc/dtle/nomad.hcl"]
