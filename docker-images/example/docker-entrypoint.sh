#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

if [[ -z "$BROKER_ID" ]]; then
    BROKER_ID=1
    echo "WARNING: Using default BROKER_ID=1, which is valid only for non-clustered installations."
fi
if [[ -z "$CONSUL_SERVICE" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Consul were named 'consul' in this container, then Docker should have created several envs,
    # such as 'CONSUL_PORT_8500_TCP'. If so, then use that to automatically set the 'consul.connect' property.
    export CONSUL_SERVICE=$(env | grep .*PORT_8500_TCP= | sed -e 's|.*tcp://||' | uniq | paste -sd ,)
fi
if [[ "x$CONSUL_SERVICE" = "x" ]]; then
    echo "The CONSUL_SERVICE variable must be set, or the container must be linked to one that runs Consul."
    exit 1
else
    echo "Using CONSUL_SERVICE=$CONSUL_SERVICE"
fi

export UDUP_CONSUL_SERVICE=$CONSUL_SERVICE
export UDUP_BROKER_ID=$BROKER_ID
unset BROKER_ID
unset CONSUL_SERVICE

if [[ -z "$ADVERTISED_PORT" ]]; then
    ADVERTISED_PORT=8190
fi
if [[ -z "$BIND_PORT" ]]; then
    BIND_PORT=8191
fi
if [[ -z "$RPC_PORT" ]]; then
    RPC_PORT=8192
fi
if [[ -z "$HOST_NAME" ]]; then
    HOST_NAME=$(ip addr | grep 'BROADCAST' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
fi

: ${PORT:=8190}
: ${ADVERTISED_PORT:=8190}
: ${BIND_PORT:=8191}
: ${RPC_PORT:=8192}
: ${ADVERTISED_HOST_NAME:=$HOST_NAME}
export UDUP_ADVERTISED_PORT=$ADVERTISED_PORT
export UDUP_BIND_PORT=$BIND_PORT
export UDUP_RPC_PORT=$RPC_PORT
export UDUP_ADVERTISED_HOST_NAME=$ADVERTISED_HOST_NAME
export UDUP_HOST_NAME=$HOST_NAME
unset PORT
unset HOST_NAME
unset ADVERTISED_HOST_PORT
unset UDUP_BIND_PORT
unset UDUP_RPC_PORT
unset ADVERTISED_HOST_NAME
echo "Using UDUP_ADVERTISED_PORT=$UDUP_ADVERTISED_PORT"
echo "Using UDUP_ADVERTISED_HOST_NAME=$UDUP_ADVERTISED_HOST_NAME"

# Process the argument to this container ...
case $1 in
    start)
        #
        # Process all environment variables that start with 'UDUP_' (but not 'UDUP_HOME' or 'UDUP_VERSION'):
        #
        for VAR in `env`
        do
          env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
          if [[ $env_var =~ ^UDUP_ && $env_var != "UDUP_VERSION" && $env_var != "UDUP_HOME"  && $env_var != "UDUP_LOG4J_OPTS" ]]; then
            prop_name=`echo "$VAR" | sed -r "s/^UDUP_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            if egrep -q "(^|^#)$prop_name=" /etc/udup/udup.conf; then
                #note that no config names or values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" /etc/udup/udup.conf
            else
                #echo "Adding property $prop_name=${!env_var}"
                echo "$prop_name=${!env_var}" >> /etc/udup/udup.conf
            fi
          fi
        done

        exec service udup start
        ;;
    stop)
        echo "Stop Udup service..."
        exec service udup stop
        ;;

esac

# Otherwise just run the specified command
exec "$@"
