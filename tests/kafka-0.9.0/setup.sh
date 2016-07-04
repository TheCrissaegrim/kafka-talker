#!/bin/bash

# set an initial value for the flag
ARG_B=0

# read the options
TEMP=`getopt -o b:i:f --long help,force-download,host-name:,broker-id:,broker-list: -n 'test.sh' -- "$@"`
eval set -- "$TEMP"

# extract options and their arguments into variables.
force_download=0
while true ; do
    case "$1" in
        -b|--broker-id)
            case "$2" in
                "") broker_id=0 ; shift 2 ;;
                *) broker_id=$2 ; shift 2 ;;
            esac ;;
        -i|--help)
            echo "Usage:"
            echo "  --broker-id: broker ID which will set in 'broker.id' parameter in 'server.properties' file."
            echo "  --broker-list: broker IDs list which will be set in 'zookeeper.connect' parameter in 'server.properties' file and in 'server.x' parameters in 'zookeeper.properties' file."
            echo "  --host-name: broker hostname which will be set in 'host.name' parameter in 'server.properties' file."
            echo "  --force-download: force file to be download."
            exit 0 ;;
        -h|--host-name)
            case "$2" in
                "") host_name='localhost' ; shift 2 ;;
                *) host_name=$2 ; shift 2 ;;
            esac ;;
        -l|--broker-list)
            case "$2" in
                "") broker_list='' ; shift 2 ;;
                *) broker_list=$2 ; shift 2 ;;
            esac ;;
        -f|--force-download)
            force_download=1
            shift 1 ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

# Split broker list on ","
SAVE_IFS="$IFS"
IFS=','
read -ra broker_list <<< "${broker_list}"    #Convert string to array
IFS="$SAVE_IFS"

# [server.properties] Build "zookeeper.connect" value
zookeeper_connect=()
for i in "${!broker_list[@]}"; do
    zookeeper_connect[$i]=${broker_list[$i]}:2181
done

# Join broker list with ","
SAVE_IFS="$IFS"
IFS=","
zookeeper_connect="${zookeeper_connect[*]}"
IFS="$SAVE_IFS"

# [zookeeper.properties] Build "server.x" values
servers=()
for i in "${!broker_list[@]}"; do
    servers[$i]="server.$(($i+1))="${broker_list[$i]}":2888:3888\n"
done

# Join server list with "\n"
SAVE_IFS="$IFS"
IFS="\n"
servers="${servers[*]}"
IFS="$SAVE_IFS"

# Get current directory path
CWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Check that Kafka binary file exists, if not download it
DOWNLOAD="N"
if [ ! -f ${CWD}/kafka_2.11-0.9.0.0.tgz ] && [ $force_download -eq 0 ]; then
    echo "File kafka_2.11-0.9.0.0.tgz not found. Do you want to download it? (Y/n)"
    read DOWNLOAD
fi

if [ "${DOWNLOAD}" == "Y" ] || [ "${DOWNLOAD}" == "y" ] || [ -z "${DOWNLOAD}" ] || [ $force_download -eq 1 ]; then
    sudo wget http://www.eu.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz -P ${CWD}/
else
    echo "ERROR: unable to continue Kafka installation without binary"
    exit 1
fi

# Create kafka user
if ! getent passwd kafka >/dev/null; then
    sudo adduser \
        --system \
        --group \
        --home /usr/share/kafka \
        --gecos "Apache Kafka" \
        --shell /bin/bash \
        kafka >/dev/null 2>/dev/null || :
fi

if ! getent passwd zookeeper >/dev/null; then
    sudo adduser \
        --system \
        --group \
        --no-create-home \
        --gecos "Apache Zookeeper" \
        --shell /bin/bash \
        zookeeper >/dev/null 2>/dev/null || :
fi

# Create directories
sudo mkdir -pv /var/lib/kafka
sudo mkdir -pv /var/lib/zookeeper
sudo mkdir -pv /var/log/kafka
sudo mkdir -pv /var/log/zookeeper
sudo touch /var/log/zookeeper/zookeeper.log
sudo mkdir -pv /etc/kafka

# Create init.d scripts
sudo cp -v ${CWD}/etc/init.d/* /etc/init.d

if [ -f /etc/init.d/kafka ]; then
    sudo chown 0:0 /etc/init.d/kafka
    sudo chmod 755 /etc/init.d/kafka
    sudo update-rc.d kafka defaults 98 02
fi

if [ -f /etc/init.d/zookeeper ]; then
    sudo chown 0:0 /etc/init.d/zookeeper
    sudo chmod 755 /etc/init.d/zookeeper
    sudo update-rc.d zookeeper defaults 98 02
fi

# Download and uncompress Kafka into kafka user home directory
cd ~kafka
rm -frv *
cp ${CWD}/kafka_2.11-0.9.0.0.tgz .
sudo tar -zxvf kafka_2.11-0.9.0.0.tgz
sudo mv -v kafka_2.11-0.9.0.0/* .
sudo rm -frv kafka_2.11-0.9.0.0*

# Copy configuration files
mv -fv ~kafka/config/* /etc/kafka/
cp -fv ${CWD}/etc/kafka/{server.properties,zookeeper.properties} /etc/kafka/
rm -frv ~kafka/config
sudo ln -sfv /etc/kafka ~kafka/config

# Create default configuration files
sudo cp -v ${CWD}/etc/default/* /etc/default

# Create logrotate configuration file
sudo cp -v ${CWD}/etc/logrotate.d/* /etc/logrotate.d

# Create security limit file
sudo cp -v ${CWD}/etc/security/limits.d/* /etc/security/limits.d

# Create PID files for kafka and zookeeper services
sudo touch /var/run/{kafka,zookeeper}.pid
sudo chown kafka:kafka /var/run/kafka.pid
#sudo chown zookeeper:zookeeper /var/run/zookeeper.pid
sudo chown kafka:kafka /var/run/zookeeper.pid
sudo chmod 644 /var/run/kafka.pid
sudo chmod 644 /var/run/zookeeper.pid

# Set configuration
sed -i "s/__BROKER_ID__/${broker_id}/g" /etc/kafka/server.properties
sed -i "s/__HOST_NAME__/${host_name}/g" /etc/kafka/server.properties
sed -i "s/__ZOOKEEPER_CONNECT__/${zookeeper_connect}/g" /etc/kafka/server.properties
sed -i "s/__SERVERS__/${servers}/g" /etc/kafka/zookeeper.properties
sudo echo "${broker_id}" > /var/lib/zookeeper/myid

# Set permissions
sudo chown -R kafka:kafka ~kafka && sudo chmod -R 775 ~kafka

sudo chmod 755 /etc/kafka
sudo chown -R 0:0 /etc/kafka && sudo chmod 644 /etc/kafka/*

sudo chown 0:0 /etc/security/limits.d/kafka-nofiles.conf
sudo chown -R kafka:kafka /var/lib/kafka
#sudo chown -R zookeeper:zookeeper /var/lib/zookeeper
sudo chown -R kafka:kafka /var/lib/zookeeper
sudo chmod -R 755 /var/lib/kafka
sudo chmod -R 755 /var/lib/zookeeper
sudo chown -R kafka:kafka /var/log/kafka
sudo chmod -R 755 /var/log/kafka
#sudo chown -R zookeeper:zookeeper /var/log/zookeeper
sudo chown -R kafka:kafka /var/log/zookeeper
sudo chmod -R 755 /var/log/zookeeper
sudo chown 0:0 /etc/default/kafka
sudo chown 0:0 /etc/logrotate.d/kafka

exit 0
