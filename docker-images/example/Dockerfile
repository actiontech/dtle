From docker-registry:5000/centos:centos7

#
# Set the version, home directory.
#
ENV DTLE_BRANCH=master \
    DTLE_VERSION=9.9.9.9 \
    DTLE_HOME=/udup

#
# Create a user and home directory for Udup
#
USER root
RUN groupadd -r udup -g 1001 && useradd -u 1001 -r -g udup -m -d $DTLE_HOME -s /sbin/nologin -c "Udup user" udup && \
    chmod 755 $DTLE_HOME

#
# Download Udup
#
RUN curl -fSL -O -u ftp:ftp ftp://10.186.18.20/actiontech-udup/qa/$DTLE_VERSION/udup-$DTLE_VERSION-qa.x86_64.rpm

#
# Install ...
#
RUN rpm -ivh udup-$DTLE_VERSION-qa.x86_64.rpm

#
# Change ownership and switch use\
#
RUN chown -R udup $DTLE_HOME && \
    chgrp -R udup $DTLE_HOME
USER udup

# Set the working directory to the Udup home directory
WORKDIR $DTLE_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 8190
VOLUME ["/udup"]

COPY ./udup.conf /etc/udup/

CMD ["/usr/bin/udup", "server", "--config", "/etc/udup/udup.conf"]
