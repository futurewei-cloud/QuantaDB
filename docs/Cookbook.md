
# Development

To further develop QuantaDB, one would modify the RC and QDB components using
the following instructions.

## build instructions

1. Checkout a copy of the QuantaDB repository:
```
    git clone https://github.com/futurewei-cloud/QuantaDB.git
```

2. Checkout the submodule within the QuantaDB repository:
```
   cd QuantaDB; git submodule update --init --recursive
```

3. Install dependencies

  Install protobuf
  (<https://gist.github.com/diegopacheco/cd795d36e6ebcd2537cd18174865887b>)

  Install OpenJDK8 (See the OpenJDK8 section)
  (<https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-ubuntu-18-04>)

  Install Boost library (`sudo apt install libboost-all-dev`)

  Install libtbb-dev (`sudo apt install libtbb-dev`)

4. Build the source: `make all`

5. Build the unit test: `make tests`

6. Build tools: `cd tools/quantadb; make`

## configurate cluster

### 1. Install packages

Install protobuf (https://gist.github.com/diegopacheco/cd795d36e6ebcd2537cd18174865887b)

```
sudo apt install Libcrecpp0v5
sudo apt install libzookeeper-mt-dev
sudo apt install libboost-all-dev
sudo apt install libtbb-dev
sudo apt install cifs-utils

```

### 2. Configure DNS

RAMCloud orchestration/test tool issues linux command remotely using
participant's name (e.g. rc1, rc2..). We can either setup local dns
server or hardcode the DNS entry on each host.

Hardcode the DSN entry on each host

Open the /etc/hosts, and append the list of participant entries.
Example\:

```
# Test cluster
192.168.1.25 rc25 node25
192.168.1.26 rc26 node26
192.168.1.27 rc27 node27
192.168.1.28 rc28 node28 rcmaster
```

### 3. Configure ssh login

RAMCloud orchestration/test tool issues multiple linux commands to the
participants during the test. It is best to setup SSH login without password.

On the host performing the orchestration

  * Generate ssh key pair: `ssh-keygen`

  * Append the public key to the participants: `ssh-copy-id ubuntu @{participating_server_ip}`

  * Verify the ssh login by ssh into each participating server

### 4. Configure memory limit

RAMCloud requires allocated memory to be in RAM and not paged out by OS.

Check the system/user memory limit using command: `ulimit -l`

  * Using a text editor to open `/etc/security/limits.conf` and append the followings for user **ubuntu**

```
ubuntu hard memlock unlimited
ubuntu soft memlock unlimited
```

  * Logout and log back in for the changes to be effective.

### 5. Share workspace

RAMCloud orchestration/test tool requires all of participants to share the same
workspace, so that each participants can write to the same log directory.

Share directory using Samba

On the samba server:

  * Install Samba: `sudo apt install samba`
  * Add sharing directory to `smb.conf`, for example:

```
[ramcloud]
comment = Ramcloud source
path = /ws #directory to be shared
read only = no
browsable= yes
valid users = ubuntu
```

  * Restart samba process: `sudo service smbd restart`
  * Request firewall to allow the samba messages to go through: `sudo ufw allow samba`
  * Add samba user: `sudo smbpasswd -a ubuntu`

On the samba client:

  * Verify samba server is setup correctly, try login using smbclient:

  `smbclient //{samba_server_ip}/ramcloud -U ubuntu`

  * Install cifs client: 
  
  `sudo apt install cifs-utils`

  * Create the mount directory (e.g. /ws):o
  
  `sudo mkdir /ws; sudo chown ubuntu /ws`

  * Mount the directory: 

  `sudo mount -t cifs -o rw -o username=ubuntu,uid=1000 //{samba_server_ip}/ramcloud /ws`

### 6. Configure Zookeeper

RAMCloud uses zookeeper to manage the services in the cluster.

Instructions for installing standalone zookeeper process:

  * Download zookeeper source: 
```
wget https://www-us.apache.org/dist/zookeeper/stable/apache-zookeeper-3.5.6-bin.tar.gz
tar -xzvf apache-zookeeper-3.5.6-bin.tar.gz`
```

  * Create a zookeeper data directory (e.g. `/var/zookeeper`)

  * Under apache-zookeeper-3.5.6-bin/conf, copy the `zoo_sample.cfg` into `zoo.cfg`
    and change the zookeeper data directory

  * Launch the zookeeper service:

`apache-zookeeper-3.5.6-bin/bin/zkServer.sh start`

  * Install zookeeper library on the participating node

`sudo apt install libzookeeper-mt-dev`

### 7.  Create a topology file

Create the `scripts/localconfig.py`:

```
# This file customizes the cluster configuration.
# It is automatically included by config.py.
hosts = []
for i in range(25,28):
  hosts.append(('rc%02d' % i,
                '192.168.1.%d' % (i),
                i))
```

## RUN performance test

There is a TPCC test available as part of the RC. It also applies to QuantaDB.

-   Help commands: `scripts/clusterperf.py --help`

-   Example (3 server nodes, in memory only, no replication:
    `scripts/clusterperf.py -T tcp --servers=3 -v -r 0 -v --disks=`

## Set up PTP

These are the steps to set up PTP.

  * Installation

`sudo apt install linuxptp`

  * Update ptp4l configuration
Edit /lib/systemd/system/ptp4l.service. Use the right ethernet device name
(e.g., eno4) for the ptp4l -i
Note: /etc/linuxptp/ptp4l.conf is where ptp4l.conf at

  * Starting ptp4l service

`sudo service ptp4l start`

  * Update phc2sys config [phc2sys syncs system clock to PTP clock]

    Edit `/lib/systemd/system/phc2sys.service`. Use the right device name

  * Starting phc2sys

`sudo service phc2sys restart`

## Collect core dump

To request apport to monitor our own process, do the followings:

  * Under user home directory (e.g. /home/ubuntu), create a file settings under
    .config/apport/ with the following content:

```
[main]
unpackaged=true
```

  * Make sure ulimit is set to unlimited. You can add the following to end of
    the .profile file.

`ulimit -c unlimited`

When a process (e.g. foo) is crashed, the system generates a file called
`foo.crash` under `/var/crash`. This is more than a core file. Here is what you do.

  * unpack the file using

`apport-unpack /var/crash/[filename].crash [destination]`

  * the actual corefile is `/[destination]/CoreDump`

If your process is launched manually (e.g. not through a script), you can skip
step 1. The core file will be generated at the directory where the process is
launched.
