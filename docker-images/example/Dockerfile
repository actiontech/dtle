From centos:centos7

#
# Set the version, home directory.
#
ENV DTLE_VERSION=9.9.9.9 \
    DTLE_HOME=/dts

#
# Create a user and home directory for Dtle
#
USER root
RUN groupadd -r dts -g 1001 && useradd -u 1001 -r -g dts -m -d $DTLE_HOME -s /sbin/nologin -c "Dtle user" dts && \
    chmod 755 $DTLE_HOME

#
# Download Dtle and Install
#
RUN curl -fSL -O -u ftp:ftp ftp://10.186.18.20/actiontech-dts/qa/$DTLE_VERSION/dts-$DTLE_VERSION-qa.x86_64.rpm && \
    rpm -ivh dts-$DTLE_VERSION-qa.x86_64.rpm && \
    rm dts-$DTLE_VERSION-qa.x86_64.rpm

#
# Change ownership and switch use\
#
RUN chown -R dts $DTLE_HOME && \
    chgrp -R dts $DTLE_HOME
USER dts

# Set the working directory to the Dtle home directory
WORKDIR $DTLE_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 8190
VOLUME ["/dts"]

COPY ./dts.conf /etc/dts/

CMD ["/usr/bin/dts", "server", "--config", "/etc/dts/dts.conf"]
