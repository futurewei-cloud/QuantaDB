# Jepsen-IO Experiment

### Contents 

- **Clojure Tutorials**
    - Understanding the programming language syntax of Clojure
    - [Reference link](https://www.tutorialspoint.com/clojure/clojure_overview.htm) 
- **Jepsen Tutorials**
    - Understanding the basic usage `Etcd` with Jepson
        - [Scaffolding](tutorial/scaffolding)
        - [Database](database)
        - [Client](client)
        - [Checker](checker)
    - [Reference link](https://github.com/jepsen-io/jepsen/blob/master/doc/tutorial/index.md)
- **[Elle Experiments](elle)**
    - [Reference link](https://github.com/jepsen-io/elle)

### Prerequisite
- Followed the [dockerized Jepson](https://github.com/jepsen-io/jepsen/tree/master/docker) provided in Jepson repo

```
# install docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# install Jepsen
git clone https://github.com/jepsen-io/jepsen.git

# move to docker tutorial
cd jepsen/docker

# removed in up.sh
set -- "${POSITIONAL[@]}" # restore positional parameters

# set up docker containers to run Jepsen
sudo ./up.sh

# login
sudo docker exec -it jepsen-control bash
```