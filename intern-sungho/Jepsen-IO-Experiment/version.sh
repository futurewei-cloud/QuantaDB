git config credential.helper store
git pull origin master
HOSTS="$(cat /etc/hostname)"
LOCATION="$(pwd)"
VERSION=3
TIMESTAMP=`date "+%Y%m%d-%H%M%S"`


if [ "$1" = "commit" ]
then
  git add .
  git commit -m "$2"
  git push origin master

elif [ "$1" = "clojure" ]
then
    if [ "$2" = "init" ]
    then
        cd /
        rm -rf tutorial
        lein new tutorial
        echo "project created"
    else
        if [ -z "$2" ]
        then
            echo "no program to run"
            exit
        fi

        if [ "$2" = "file" ]
        then
            cp clojure/file/example.txt /tutorial/
        fi

        cp clojure/$2/project.clj /tutorial/project.clj
        cp clojure/$2/core.clj /tutorial/src/tutorial/core.clj
        cd /tutorial
        lein run
    fi

# ./version tutorial ( scaffolding, database, client, checker(+ time-limit), elle)
elif [ "$1" = "tutorial" ]
then

    if [ -z "$2" ]
    then
        echo "no program to run"
        exit
    fi

    # copy the dependencies
    if [ "$2" = "scaffolding" ]
    then
        cd /jepsen/
        rm -rf jepsen.etcdemo
        lein new jepsen.etcdemo

        cd /jepsen/Jepsen-IO-Experiment/
        cp tutorial/scaffolding/project.clj /jepsen/jepsen.etcdemo/project.clj
    fi

    # copy the main function
    cp tutorial/$2/etcdemo.clj /jepsen/jepsen.etcdemo/src/jepsen/etcdemo.clj

    # invoke the main function
    cd /jepsen/jepsen.etcdemo
    lein run test
    echo "completed $2 test case"

    # migrate the invalid results
    if test -f "/jepsen.etcdemo/store/latest/linear.svg"; then
        echo "invalid result detected!"
        cd /jepsen/Jepsen-IO-Experiment/
        echo "storing them in debug directory ..."
        cp /jepsen/jepsen.etcdemo/store/latest/linear.svg debug/linear-$TIMESTAMP.svg
        cp /jepsen/jepsen.etcdemo/store/latest/history.txt debug/history-$TIMESTAMP.txt
    fi


elif [ "$1" = "elle" ]
then

    if [ -z "$2" ]
    then
        echo "no directory to run"
        exit
    fi


    if [ "$2" = "outside_docker" ]
    then
        echo "outside the container"
        echo "git clone https://github.com/jepsen-io/jepsen"
        echo "commit 556d30549c469d5a39a0186d7e5e2af4a014d8b5, branch master, Date: Mon Jun 29 12:07:08 2020 -0400"
        echo "[Update]: Modified installation of git inside every node"
        cp elle/update/docker/Dockerfile /home/users/sungho/jepsen/docker/node/Dockerfile
        exit

    elif [ "$2" = "inside_docker" ]
    then
        echo "git clone https://github.com/jepsen-io/redis.git"
        echo "commit 6c857fb16f7977cff21756acc99eb302cfdd11bd, branch: master, Date: Tue Jun 23 10:17:12 2020 -0400"
        echo "[Update]: modify to a valid link to the git repo"
        cp elle/update/jepsen/db.clj /jepsen/redis/src/jepsen/redis/
        exit
    fi

    if [ -z "$3" ]
    then
        echo "no command line (test-all) for redis"
        exit
    fi

    files=("append.clj" "client.clj" "core.clj" "db.clj" "nemesis.clj")
    for file in "${files[@]}"
    do
        if test -f "elle/$2/$file"; then
            echo "copied elle/$2/$file"
            cp elle/$2/$file  /jepsen/redis/src/jepsen/redis/
        fi
    done

    # uses redis repository
    cd /jepsen/redis
    lein install
    lein run $3 $4 $5 # test-all


elif [ "$1" = "elle_mongodb" ]
then
    cp elle/mongodb/mongodb.clj /jepsen/mongodb/src/jepsen/
    cp elle/mongodb/db.clj /jepsen/mongodb/src/jepsen/mongodb/
    cp elle/mongodb/hostnames /jepsen/mongodb/
    cd /jepsen/mongodb
    lein install
    lein run test-all -w list-append --nodes-file hostnames -r 1000 --concurrency 3n --time-limit 120 --read-concern majority --nemesis-interval 1 --nemesis partition

elif [ "$1" = "demo" ]
then
    cd /jepsen/redis
    lein run $2 # test-all
else
  echo "no argument"

fi
